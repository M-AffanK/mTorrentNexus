#include <arpa/inet.h>
#include <netdb.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <cctype>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <exception>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <map>
#include <mutex>
#include <optional>
#include <set>
#include <sstream>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

namespace {

std::vector<std::string> split(const std::string& text, char delimiter = ' ') {
    std::vector<std::string> parts;
    std::stringstream ss(text);
    std::string item;
    while (std::getline(ss, item, delimiter)) {
        if (!item.empty()) {
            parts.push_back(item);
        }
    }
    return parts;
}

std::string trim(const std::string& s) {
    std::size_t start = 0;
    while (start < s.size() && std::isspace(static_cast<unsigned char>(s[start])) != 0) {
        ++start;
    }
    std::size_t end = s.size();
    while (end > start && std::isspace(static_cast<unsigned char>(s[end - 1])) != 0) {
        --end;
    }
    return s.substr(start, end - start);
}

std::string toLower(std::string s) {
    for (char& c : s) {
        c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
    }
    return s;
}

std::string escapeField(const std::string& input) {
    std::string out;
    out.reserve(input.size() + 8);
    for (const char c : input) {
        switch (c) {
            case '\\':
                out += "\\\\";
                break;
            case '\t':
                out += "\\t";
                break;
            case '\n':
                out += "\\n";
                break;
            default:
                out.push_back(c);
                break;
        }
    }
    return out;
}

std::string unescapeField(const std::string& input) {
    std::string out;
    out.reserve(input.size());
    for (std::size_t i = 0; i < input.size(); ++i) {
        if (input[i] == '\\' && i + 1 < input.size()) {
            const char next = input[i + 1];
            if (next == 't') {
                out.push_back('\t');
                ++i;
                continue;
            }
            if (next == 'n') {
                out.push_back('\n');
                ++i;
                continue;
            }
            if (next == '\\') {
                out.push_back('\\');
                ++i;
                continue;
            }
        }
        out.push_back(input[i]);
    }
    return out;
}

bool writeAll(int fd, const void* data, std::size_t size) {
    const char* ptr = static_cast<const char*>(data);
    std::size_t sent = 0;
    while (sent < size) {
        const ssize_t n = send(fd, ptr + sent, size - sent, 0);
        if (n <= 0) {
            return false;
        }
        sent += static_cast<std::size_t>(n);
    }
    return true;
}

bool writeAll(int fd, const std::string& text) {
    return writeAll(fd, text.data(), text.size());
}

bool readExact(int fd, std::vector<char>& out, std::size_t size) {
    out.assign(size, 0);
    std::size_t got = 0;
    while (got < size) {
        const ssize_t n = recv(fd, out.data() + got, size - got, 0);
        if (n <= 0) {
            return false;
        }
        got += static_cast<std::size_t>(n);
    }
    return true;
}

std::optional<std::string> readLine(int fd) {
    std::string line;
    char c = 0;
    while (true) {
        const ssize_t n = recv(fd, &c, 1, 0);
        if (n <= 0) {
            return std::nullopt;
        }
        if (c == '\n') {
            break;
        }
        if (c != '\r') {
            line.push_back(c);
        }
    }
    return line;
}

int connectTcp(const std::string& host, uint16_t port) {
    struct addrinfo hints;
    std::memset(&hints, 0, sizeof(hints));
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;

    struct addrinfo* result = nullptr;
    const std::string service = std::to_string(port);
    if (getaddrinfo(host.c_str(), service.c_str(), &hints, &result) != 0) {
        return -1;
    }

    int fd = -1;
    for (struct addrinfo* rp = result; rp != nullptr; rp = rp->ai_next) {
        fd = socket(rp->ai_family, rp->ai_socktype, rp->ai_protocol);
        if (fd == -1) {
            continue;
        }
        if (connect(fd, rp->ai_addr, rp->ai_addrlen) == 0) {
            break;
        }
        close(fd);
        fd = -1;
    }

    freeaddrinfo(result);
    return fd;
}

struct StorageNode {
    std::string id;
    std::string host;
    uint16_t port = 0;
    std::uint64_t capacityBytes = 0;
};

struct ChunkPlacement {
    std::uint32_t index = 0;
    std::uint64_t size = 0;
    std::vector<std::string> nodeIds;
};

struct FileRecord {
    std::string fileId;
    std::string fileName;
    std::uint64_t fileSize = 0;
    std::uint32_t chunkSize = 0;
    std::uint32_t chunkCount = 0;
    std::string shareToken;
    std::vector<ChunkPlacement> chunks;
};

struct UploadPlan {
    std::string uploadId;
    std::string fileId;
    std::string fileName;
    std::uint64_t fileSize = 0;
    std::uint32_t chunkSize = 0;
    std::uint32_t chunkCount = 0;
    std::vector<ChunkPlacement> chunks;
    std::set<std::uint32_t> uploadedIndices;
};

class CoordinatorState {
public:
    CoordinatorState(std::size_t replicationFactor, std::string metadataPath)
        : replicationFactor_(replicationFactor == 0 ? 1 : replicationFactor), metadataPath_(std::move(metadataPath)) {
        loadMetadata();
    }

    void registerNode(const StorageNode& node) {
        std::scoped_lock<std::mutex> lock(mu_);
        nodes_[node.id] = node;
    }

    std::optional<UploadPlan> createUploadPlan(const std::string& fileName,
                                               std::uint64_t fileSize,
                                               std::uint32_t chunkSize,
                                               std::uint32_t chunkCount) {
        std::scoped_lock<std::mutex> lock(mu_);
        if (nodes_.empty()) {
            return std::nullopt;
        }

        UploadPlan plan;
        plan.uploadId = nextId("upl");
        plan.fileId = nextId("file");
        plan.fileName = fileName;
        plan.fileSize = fileSize;
        plan.chunkSize = chunkSize;
        plan.chunkCount = chunkCount;

        std::vector<std::string> nodeOrder;
        nodeOrder.reserve(nodes_.size());
        for (const auto& [id, _] : nodes_) {
            nodeOrder.push_back(id);
        }
        std::sort(nodeOrder.begin(), nodeOrder.end());

        const std::size_t replicas = std::min(replicationFactor_, nodeOrder.size());
        for (std::uint32_t i = 0; i < chunkCount; ++i) {
            ChunkPlacement placement;
            placement.index = i;
            const std::uint64_t offset = static_cast<std::uint64_t>(i) * chunkSize;
            placement.size = std::min<std::uint64_t>(chunkSize, fileSize - offset);
            for (std::size_t r = 0; r < replicas; ++r) {
                placement.nodeIds.push_back(nodeOrder[(i + r) % nodeOrder.size()]);
            }
            plan.chunks.push_back(placement);
        }

        uploads_[plan.uploadId] = plan;
        return plan;
    }

    std::optional<UploadPlan> getUpload(const std::string& uploadId) const {
        std::scoped_lock<std::mutex> lock(mu_);
        const auto it = uploads_.find(uploadId);
        if (it == uploads_.end()) {
            return std::nullopt;
        }
        return it->second;
    }

    bool markChunkUploaded(const std::string& uploadId, std::uint32_t chunkIndex) {
        std::scoped_lock<std::mutex> lock(mu_);
        auto it = uploads_.find(uploadId);
        if (it == uploads_.end() || chunkIndex >= it->second.chunkCount) {
            return false;
        }
        it->second.uploadedIndices.insert(chunkIndex);
        return true;
    }

    std::optional<FileRecord> finalizeUpload(const std::string& uploadId) {
        std::scoped_lock<std::mutex> lock(mu_);
        auto it = uploads_.find(uploadId);
        if (it == uploads_.end()) {
            return std::nullopt;
        }
        if (it->second.uploadedIndices.size() != it->second.chunkCount) {
            return std::nullopt;
        }

        FileRecord file;
        file.fileId = it->second.fileId;
        file.fileName = it->second.fileName;
        file.fileSize = it->second.fileSize;
        file.chunkSize = it->second.chunkSize;
        file.chunkCount = it->second.chunkCount;
        file.shareToken = nextId("shr");
        file.chunks = it->second.chunks;

        files_[file.fileId] = file;
        shareToFile_[file.shareToken] = file.fileId;
        uploads_.erase(it);
        saveMetadataLocked();
        return file;
    }

    std::optional<FileRecord> getFile(const std::string& fileId) const {
        std::scoped_lock<std::mutex> lock(mu_);
        const auto it = files_.find(fileId);
        if (it == files_.end()) {
            return std::nullopt;
        }
        return it->second;
    }

    std::optional<FileRecord> getFileByShareToken(const std::string& shareToken) const {
        std::scoped_lock<std::mutex> lock(mu_);
        const auto it = shareToFile_.find(shareToken);
        if (it == shareToFile_.end()) {
            return std::nullopt;
        }
        const auto fit = files_.find(it->second);
        if (fit == files_.end()) {
            return std::nullopt;
        }
        return fit->second;
    }

    std::vector<FileRecord> listFiles() const {
        std::scoped_lock<std::mutex> lock(mu_);
        std::vector<FileRecord> out;
        out.reserve(files_.size());
        for (const auto& [_, file] : files_) {
            out.push_back(file);
        }
        return out;
    }

    std::vector<FileRecord> searchFilesByName(const std::string& query) const {
        std::scoped_lock<std::mutex> lock(mu_);
        const std::string q = toLower(query);
        std::vector<FileRecord> out;
        for (const auto& [_, file] : files_) {
            if (toLower(file.fileName).find(q) != std::string::npos) {
                out.push_back(file);
            }
        }
        return out;
    }

    std::optional<std::string> shareUrlForFile(const std::string& fileId) const {
        std::scoped_lock<std::mutex> lock(mu_);
        const auto it = files_.find(fileId);
        if (it == files_.end()) {
            return std::nullopt;
        }
        return "mtorrent://share/" + it->second.shareToken;
    }

    std::optional<StorageNode> nodeById(const std::string& id) const {
        std::scoped_lock<std::mutex> lock(mu_);
        const auto it = nodes_.find(id);
        if (it == nodes_.end()) {
            return std::nullopt;
        }
        return it->second;
    }

private:
    std::string nextId(const std::string& prefix) {
        std::ostringstream os;
        os << prefix << "_" << ++counter_;
        return os.str();
    }

    void loadMetadata() {
        std::scoped_lock<std::mutex> lock(mu_);
        files_.clear();
        shareToFile_.clear();

        if (!std::filesystem::exists(metadataPath_)) {
            return;
        }

        std::ifstream in(metadataPath_);
        if (!in.is_open()) {
            return;
        }

        std::string line;
        while (std::getline(in, line)) {
            if (line.empty()) {
                continue;
            }
            if (line.rfind("COUNTER\t", 0) == 0) {
                const auto fields = split(line, '\t');
                if (fields.size() >= 2) {
                    counter_ = std::stoull(fields[1]);
                }
                continue;
            }
            if (line.rfind("FILE\t", 0) == 0) {
                const auto fields = split(line, '\t');
                if (fields.size() < 7) {
                    continue;
                }
                FileRecord file;
                file.fileId = fields[1];
                file.fileName = unescapeField(fields[2]);
                file.fileSize = std::stoull(fields[3]);
                file.chunkSize = static_cast<std::uint32_t>(std::stoul(fields[4]));
                file.chunkCount = static_cast<std::uint32_t>(std::stoul(fields[5]));
                file.shareToken = fields[6];

                while (std::getline(in, line)) {
                    if (line == "ENDFILE") {
                        break;
                    }
                    if (line.rfind("CHUNK\t", 0) != 0) {
                        continue;
                    }
                    const auto cf = split(line, '\t');
                    if (cf.size() < 5 || cf[1] != file.fileId) {
                        continue;
                    }
                    ChunkPlacement chunk;
                    chunk.index = static_cast<std::uint32_t>(std::stoul(cf[2]));
                    chunk.size = std::stoull(cf[3]);
                    chunk.nodeIds = split(cf[4], ',');
                    file.chunks.push_back(chunk);
                }

                files_[file.fileId] = file;
                if (!file.shareToken.empty()) {
                    shareToFile_[file.shareToken] = file.fileId;
                }
            }
        }
    }

    void saveMetadataLocked() const {
        std::ofstream out(metadataPath_, std::ios::trunc);
        if (!out.is_open()) {
            return;
        }

        out << "COUNTER\t" << counter_.load() << "\n";
        for (const auto& [_, file] : files_) {
            out << "FILE\t" << file.fileId << "\t" << escapeField(file.fileName) << "\t"
                << file.fileSize << "\t" << file.chunkSize << "\t" << file.chunkCount
                << "\t" << file.shareToken << "\n";
            for (const auto& chunk : file.chunks) {
                out << "CHUNK\t" << file.fileId << "\t" << chunk.index << "\t" << chunk.size << "\t";
                for (std::size_t i = 0; i < chunk.nodeIds.size(); ++i) {
                    if (i > 0) {
                        out << ",";
                    }
                    out << chunk.nodeIds[i];
                }
                out << "\n";
            }
            out << "ENDFILE\n";
        }
    }

    std::size_t replicationFactor_;
    std::string metadataPath_;
    mutable std::mutex mu_;
    std::unordered_map<std::string, StorageNode> nodes_;
    std::unordered_map<std::string, UploadPlan> uploads_;
    std::unordered_map<std::string, FileRecord> files_;
    std::unordered_map<std::string, std::string> shareToFile_;
    std::atomic<std::uint64_t> counter_{0};
};

class CoordinatorServer {
public:
    CoordinatorServer(uint16_t port, std::size_t replicationFactor, std::string metadataPath)
        : port_(port), state_(replicationFactor, std::move(metadataPath)) {}

    bool run() {
        listenFd_ = socket(AF_INET, SOCK_STREAM, 0);
        if (listenFd_ < 0) {
            return false;
        }

        int yes = 1;
        setsockopt(listenFd_, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(yes));

        sockaddr_in addr;
        std::memset(&addr, 0, sizeof(addr));
        addr.sin_family = AF_INET;
        addr.sin_addr.s_addr = INADDR_ANY;
        addr.sin_port = htons(port_);

        if (bind(listenFd_, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0) {
            return false;
        }
        if (listen(listenFd_, 64) < 0) {
            return false;
        }

        std::cout << "Coordinator listening on " << port_ << "\n";
        while (true) {
            sockaddr_in clientAddr;
            socklen_t len = sizeof(clientAddr);
            int clientFd = accept(listenFd_, reinterpret_cast<sockaddr*>(&clientAddr), &len);
            if (clientFd < 0) {
                continue;
            }
            std::thread(&CoordinatorServer::handleClient, this, clientFd).detach();
        }
    }

private:
    bool streamFileToClient(int fd, const FileRecord& file) {
        std::ostringstream head;
        head << "OK " << file.fileName << " " << file.fileSize << " "
             << file.chunkSize << " " << file.chunkCount << "\n"; // << file.fileId << " "
        if (!writeAll(fd, head.str())) {
            return false;
        }

        for (const auto& chunk : file.chunks) {
            std::optional<std::vector<char>> data;
            for (const auto& nodeId : chunk.nodeIds) {
                const auto node = state_.nodeById(nodeId);
                if (!node.has_value()) {
                    continue;
                }
                data = fetchChunkFromNode(*node, file.fileId, chunk.index);
                if (data.has_value()) {
                    break;
                }
            }
            if (!data.has_value()) {
                return writeAll(fd, "ERR missing_chunk\n");
            }
            std::ostringstream meta;
            meta << "CHUNK " << chunk.index << " " << data->size() << "\n";
            if (!writeAll(fd, meta.str()) || !writeAll(fd, data->data(), data->size())) {
                return false;
            }
        }
        return writeAll(fd, "END_FILE\n");
    }

    bool storeChunkOnNode(const StorageNode& node,
                          const std::string& fileId,
                          std::uint32_t chunkIndex,
                          const std::vector<char>& payload) {
        int fd = connectTcp(node.host, node.port);
        if (fd < 0) {
            return false;
        }
        std::ostringstream command;
        command << "STORE_CHUNK " << fileId << " " << chunkIndex << " " << payload.size() << "\n";
        if (!writeAll(fd, command.str()) || !writeAll(fd, payload.data(), payload.size())) {
            close(fd);
            return false;
        }
        const auto ack = readLine(fd);
        close(fd);
        return ack.has_value() && ack.value() == "OK";
    }

    std::optional<std::vector<char>> fetchChunkFromNode(const StorageNode& node,
                                                        const std::string& fileId,
                                                        std::uint32_t chunkIndex) {
        int fd = connectTcp(node.host, node.port);
        if (fd < 0) {
            return std::nullopt;
        }
        std::ostringstream command;
        command << "GET_CHUNK " << fileId << " " << chunkIndex << "\n";
        if (!writeAll(fd, command.str())) {
            close(fd);
            return std::nullopt;
        }
        const auto meta = readLine(fd);
        if (!meta.has_value()) {
            close(fd);
            return std::nullopt;
        }
        const auto parts = split(*meta);
        if (parts.size() != 2 || parts[0] != "CHUNK_DATA") {
            close(fd);
            return std::nullopt;
        }
        const std::size_t size = static_cast<std::size_t>(std::stoull(parts[1]));
        std::vector<char> data;
        if (!readExact(fd, data, size)) {
            close(fd);
            return std::nullopt;
        }
        close(fd);
        return data;
    }

    bool handleCommand(int fd, const std::string& line) {
        const auto parts = split(line);
        if (parts.empty()) {
            return true;
        }
        const std::string& cmd = parts[0];

        if (cmd == "PING") {
            return writeAll(fd, "PONG\n");
        }
        if (cmd == "REGISTER_NODE") {
            if (parts.size() != 5) {
                return writeAll(fd, "ERR usage REGISTER_NODE <id> <host> <port> <capacity>\n");
            }
            StorageNode node;
            node.id = parts[1];
            node.host = parts[2];
            node.port = static_cast<uint16_t>(std::stoi(parts[3]));
            node.capacityBytes = std::stoull(parts[4]);
            state_.registerNode(node);
            return writeAll(fd, "OK node_registered\n");
        }
        if (cmd == "UPLOAD_INIT") {
            if (parts.size() != 5) {
                return writeAll(fd, "ERR usage UPLOAD_INIT <filename> <filesize> <chunksize> <chunkcount>\n");
            }
            const auto plan = state_.createUploadPlan(parts[1], std::stoull(parts[2]),
                                                      static_cast<std::uint32_t>(std::stoul(parts[3])),
                                                      static_cast<std::uint32_t>(std::stoul(parts[4])));
            if (!plan.has_value()) {
                return writeAll(fd, "ERR no_storage_nodes\n");
            }

            std::ostringstream out;
            out << "OK " << plan->uploadId << " " << plan->fileId << "\n";
            for (const auto& chunk : plan->chunks) {
                out << "CHUNK " << chunk.index << " " << chunk.nodeIds.size();
                for (const auto& nodeId : chunk.nodeIds) {
                    out << " " << nodeId;
                }
                out << "\n";
            }
            out << "END_PLAN\n";
            return writeAll(fd, out.str());
        }
        if (cmd == "UPLOAD_CHUNK") {
            if (parts.size() != 4) {
                return writeAll(fd, "ERR usage UPLOAD_CHUNK <uploadId> <chunkIndex> <bytes>\n");
            }
            const std::string uploadId = parts[1];
            const std::uint32_t chunkIndex = static_cast<std::uint32_t>(std::stoul(parts[2]));
            const std::size_t bytes = static_cast<std::size_t>(std::stoull(parts[3]));
            std::vector<char> payload;
            if (!readExact(fd, payload, bytes)) {
                return false;
            }

            const auto plan = state_.getUpload(uploadId);
            if (!plan.has_value() || chunkIndex >= plan->chunks.size()) {
                return writeAll(fd, "ERR invalid_upload_or_chunk\n");
            }

            std::size_t success = 0;
            for (const auto& nodeId : plan->chunks[chunkIndex].nodeIds) {
                const auto node = state_.nodeById(nodeId);
                if (node.has_value() && storeChunkOnNode(*node, plan->fileId, chunkIndex, payload)) {
                    ++success;
                }
            }
            if (success == 0) {
                return writeAll(fd, "ERR chunk_store_failed\n");
            }
            state_.markChunkUploaded(uploadId, chunkIndex);
            return writeAll(fd, "OK chunk_stored\n");
        }
        if (cmd == "UPLOAD_FINISH") {
            if (parts.size() != 2) {
                return writeAll(fd, "ERR usage UPLOAD_FINISH <uploadId>\n");
            }
            const auto file = state_.finalizeUpload(parts[1]);
            if (!file.has_value()) {
                return writeAll(fd, "ERR upload_incomplete_or_missing\n");
            }
            std::ostringstream out;
            out << "OK " << file->fileId << " mtorrent://share/" << file->shareToken << "\n";
            return writeAll(fd, out.str());
        }
        if (cmd == "DOWNLOAD") {
            if (parts.size() != 2) {
                return writeAll(fd, "ERR usage DOWNLOAD <fileId>\n");
            }
            const auto file = state_.getFile(parts[1]);
            if (!file.has_value()) {
                return writeAll(fd, "ERR file_not_found\n");
            }
            return streamFileToClient(fd, *file);
        }
        if (cmd == "DOWNLOAD_SHARE") {
            if (parts.size() != 2) {
                return writeAll(fd, "ERR usage DOWNLOAD_SHARE <shareToken>\n");
            }
            std::string token = parts[1];
            const std::string prefix = "mtorrent://share/";
            if (token.rfind(prefix, 0) == 0) {
                token = token.substr(prefix.size());
            }
            const auto file = state_.getFileByShareToken(token);
            if (!file.has_value()) {
                return writeAll(fd, "ERR share_not_found\n");
            }
            return streamFileToClient(fd, *file);
        }
        if (cmd == "LIST_FILES") {
            const auto files = state_.listFiles();
            std::ostringstream out;
            out << "OK " << files.size() << "\n";
            for (const auto& file : files) {
                out << "FILE " << file.fileId << " " << file.fileName << " "
                    << file.fileSize << " " << file.chunkCount << " "
                    << "mtorrent://share/" << file.shareToken << "\n";
            }
            out << "END_LIST\n";
            return writeAll(fd, out.str());
        }
        if (cmd == "SEARCH_FILES") {
            std::string query;
            if (line.size() > std::string("SEARCH_FILES").size()) {
                query = trim(line.substr(std::string("SEARCH_FILES").size()));
            }
            if (query.empty()) {
                return writeAll(fd, "ERR usage SEARCH_FILES <name-substring>\n");
            }
            const auto files = state_.searchFilesByName(query);
            std::ostringstream out;
            out << "OK " << files.size() << "\n";
            for (const auto& file : files) {
                out << "FILE " << file.fileId << " " << file.fileName << " "
                    << file.fileSize << " " << file.chunkCount << " "
                    << "mtorrent://share/" << file.shareToken << "\n";
            }
            out << "END_SEARCH\n";
            return writeAll(fd, out.str());
        }
        if (cmd == "GET_SHARE_LINK") {
            if (parts.size() != 2) {
                return writeAll(fd, "ERR usage GET_SHARE_LINK <fileId>\n");
            }
            const auto url = state_.shareUrlForFile(parts[1]);
            if (!url.has_value()) {
                return writeAll(fd, "ERR file_not_found\n");
            }
            std::ostringstream out;
            out << "OK " << *url << "\n";
            return writeAll(fd, out.str());
        }

        return writeAll(fd, "ERR unknown_command\n");
    }

    void handleClient(int fd) {
        try {
            while (true) {
                const auto line = readLine(fd);
                if (!line.has_value()) {
                    break;
                }
                if (!handleCommand(fd, *line)) {
                    break;
                }
            }
        } catch (const std::exception& ex) {
            std::cerr << "[session] error: " << ex.what() << "\n";
        }
        close(fd);
    }

    uint16_t port_;
    int listenFd_ = -1;
    CoordinatorState state_;
};

}  // namespace

int main(int argc, char** argv) {
    uint16_t port = 9090;
    std::size_t replication = 2;
    std::string metadataPath = "server_metadata.db";

    if (argc > 1) {
        port = static_cast<uint16_t>(std::stoi(argv[1]));
    }
    if (argc > 2) {
        replication = static_cast<std::size_t>(std::stoul(argv[2]));
    }
    if (argc > 3) {
        metadataPath = argv[3];
    }

    CoordinatorServer server(port, replication, metadataPath);
    if (!server.run()) {
        std::cerr << "Failed to start coordinator server\n";
        return EXIT_FAILURE;
    }
    return EXIT_SUCCESS;
}
