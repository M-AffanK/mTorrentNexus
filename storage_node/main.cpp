#include <arpa/inet.h>
#include <netdb.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>

#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <mutex>
#include <optional>
#include <sstream>
#include <string>
#include <thread>
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

std::string safeFileName(const std::string& fileId, std::uint32_t chunkIndex) {
    std::ostringstream out;
    out << fileId << "_" << chunkIndex << ".chunk";
    return out.str();
}

class ChunkStore {
public:
    explicit ChunkStore(std::string rootDir) : rootDir_(std::move(rootDir)) {
        std::filesystem::create_directories(rootDir_);
    }

    bool storeChunk(const std::string& fileId, std::uint32_t chunkIndex, const std::vector<char>& data) {
        std::scoped_lock<std::mutex> lock(mu_);
        const auto path = std::filesystem::path(rootDir_) / safeFileName(fileId, chunkIndex);
        std::ofstream out(path, std::ios::binary | std::ios::trunc);
        if (!out.is_open()) {
            return false;
        }
        out.write(data.data(), static_cast<std::streamsize>(data.size()));
        return out.good();
    }

    std::optional<std::vector<char>> getChunk(const std::string& fileId, std::uint32_t chunkIndex) const {
        std::scoped_lock<std::mutex> lock(mu_);
        const auto path = std::filesystem::path(rootDir_) / safeFileName(fileId, chunkIndex);
        if (!std::filesystem::exists(path)) {
            return std::nullopt;
        }
        std::ifstream in(path, std::ios::binary);
        if (!in.is_open()) {
            return std::nullopt;
        }
        in.seekg(0, std::ios::end);
        const auto size = in.tellg();
        in.seekg(0, std::ios::beg);
        if (size < 0) {
            return std::nullopt;
        }
        std::vector<char> data(static_cast<std::size_t>(size));
        if (!data.empty()) {
            in.read(data.data(), size);
            if (!in.good() && !in.eof()) {
                return std::nullopt;
            }
        }
        return data;
    }

private:
    std::string rootDir_;
    mutable std::mutex mu_;
};

class StorageNodeServer {
public:
    StorageNodeServer(uint16_t port, std::string nodeId, std::string host, std::string storageDir)
        : port_(port), nodeId_(std::move(nodeId)), host_(std::move(host)), store_(std::move(storageDir)) {}

    bool registerWithCoordinator(const std::string& coordinatorHost, uint16_t coordinatorPort, std::uint64_t capacityBytes) {
        const int fd = connectTcp(coordinatorHost, coordinatorPort);
        if (fd < 0) {
            return false;
        }
        std::ostringstream cmd;
        cmd << "REGISTER_NODE " << nodeId_ << " " << host_ << " " << port_ << " " << capacityBytes << "\n";
        if (!writeAll(fd, cmd.str())) {
            close(fd);
            return false;
        }
        const auto reply = readLine(fd);
        close(fd);
        return reply.has_value() && reply.value().rfind("OK", 0) == 0;
    }

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

        std::cout << "Storage node " << nodeId_ << " listening on " << port_ << "\n";
        while (true) {
            sockaddr_in clientAddr;
            socklen_t len = sizeof(clientAddr);
            int clientFd = accept(listenFd_, reinterpret_cast<sockaddr*>(&clientAddr), &len);
            if (clientFd < 0) {
                continue;
            }
            std::thread(&StorageNodeServer::handleClient, this, clientFd).detach();
        }
    }

private:
    bool handleCommand(int fd, const std::string& line) {
        const auto parts = split(line);
        if (parts.empty()) {
            return true;
        }

        if (parts[0] == "PING") {
            return writeAll(fd, "PONG\n");
        }
        if (parts[0] == "STORE_CHUNK") {
            if (parts.size() != 4) {
                return writeAll(fd, "ERR usage STORE_CHUNK <fileId> <chunkIndex> <bytes>\n");
            }
            const std::string fileId = parts[1];
            const std::uint32_t chunkIndex = static_cast<std::uint32_t>(std::stoul(parts[2]));
            const std::size_t bytes = static_cast<std::size_t>(std::stoull(parts[3]));
            std::vector<char> data;
            if (!readExact(fd, data, bytes)) {
                return false;
            }
            if (!store_.storeChunk(fileId, chunkIndex, data)) {
                return writeAll(fd, "ERR store_failed\n");
            }
            return writeAll(fd, "OK\n");
        }
        if (parts[0] == "GET_CHUNK") {
            if (parts.size() != 3) {
                return writeAll(fd, "ERR usage GET_CHUNK <fileId> <chunkIndex>\n");
            }
            const std::string fileId = parts[1];
            const std::uint32_t chunkIndex = static_cast<std::uint32_t>(std::stoul(parts[2]));
            const auto data = store_.getChunk(fileId, chunkIndex);
            if (!data.has_value()) {
                return writeAll(fd, "ERR chunk_not_found\n");
            }
            std::ostringstream header;
            header << "CHUNK_DATA " << data->size() << "\n";
            return writeAll(fd, header.str()) && writeAll(fd, data->data(), data->size());
        }

        return writeAll(fd, "ERR unknown_command\n");
    }

    void handleClient(int fd) {
        while (true) {
            const auto line = readLine(fd);
            if (!line.has_value()) {
                break;
            }
            if (!handleCommand(fd, *line)) {
                break;
            }
        }
        close(fd);
    }

    uint16_t port_;
    std::string nodeId_;
    std::string host_;
    ChunkStore store_;
    int listenFd_ = -1;
};

}  // namespace

int main(int argc, char** argv) {
    if (argc < 8) {
        std::cerr << "Usage:\n"
                  << "  mtorrent_storage_node <node_id> <listen_port> <storage_dir> "
                  << "<coordinator_host> <coordinator_port> <advertised_host> <capacity_bytes>\n";
        return EXIT_FAILURE;
    }

    const std::string nodeId = argv[1];
    const uint16_t listenPort = static_cast<uint16_t>(std::stoi(argv[2]));
    const std::string storageDir = argv[3];
    const std::string coordinatorHost = argv[4];
    const uint16_t coordinatorPort = static_cast<uint16_t>(std::stoi(argv[5]));
    const std::string advertisedHost = argv[6];
    const std::uint64_t capacityBytes = std::stoull(argv[7]);

    StorageNodeServer node(listenPort, nodeId, advertisedHost, storageDir);

    if (!node.registerWithCoordinator(coordinatorHost, coordinatorPort, capacityBytes)) {
        std::cerr << "Failed to register with coordinator\n";
        return EXIT_FAILURE;
    }
    std::cout << "Node registered with coordinator\n";

    if (!node.run()) {
        std::cerr << "Failed to start storage node server\n";
        return EXIT_FAILURE;
    }
    return EXIT_SUCCESS;
}
