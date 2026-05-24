#include "server/storage_node.hpp"

storage_node::storage_node(uint16_t port, std::string node_id, std::string host, std::string storage_dir)
: port_(port), node_id_(std::move(node_id)), host_(std::move(host)), store_(std::move(storage_dir)) {}

bool storage_node::register_with_coordinator(const std::string& coordinator_host,
		uint16_t coordinator_port, std::uint64_t capacity_bytes) {
	const int fd = connect_tcp(coordinator_host, coordinator_port);
	if (fd < 0) {
		return false;
	}
	std::ostringstream cmd;
	cmd << "REGISTER_NODE " << node_id_ << " " << host_ << " " << port_ << " " << capacity_bytes << "\n";
	if (!write_all(fd, cmd.str())) {
		close(fd);
		return false;
	}
	const auto reply = read_line(fd);
	close(fd);
	return reply.has_value() && reply.value().rfind("OK", 0) == 0;
}

bool storage_node::run() {
	listen_fd_ = socket(AF_INET, SOCK_STREAM, 0);
	if (listen_fd_ < 0) {
		return false;
	}
	int yes = 1;
	setsockopt(listen_fd_, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(yes));

	sockaddr_in addr;
	std::memset(&addr, 0, sizeof(addr));
	addr.sin_family = AF_INET;
	addr.sin_addr.s_addr = INADDR_ANY;
	addr.sin_port = htons(port_);

	if (bind(listen_fd_, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0) {
		return false;
	}
	if (listen(listen_fd_, 64) < 0) {
		return false;
	}

	std::cout << "Storage node " << node_id_ << " listening on " << port_ << "\n";
	while (true) {
		sockaddr_in client_addr;
		socklen_t len = sizeof(client_addr);
		int client_fd = accept(listen_fd_, reinterpret_cast<sockaddr*>(&client_addr), &len);
		if (client_fd < 0) {
			continue;
		}
		std::thread(&storage_node::handle_client, this, client_fd).detach();
	}
}

bool storage_node::handle_command(int fd, const std::string& line) {
	const auto parts = split(line);
	if (parts.empty()) {
		return true;
	}

	if (parts[0] == "PING") {
		return write_all(fd, "PONG\n");
	}
	if (parts[0] == "STORE_CHUNK") {
		if (parts.size() != 4) {
			return write_all(fd, "ERR usage STORE_CHUNK <file_id> <chunk_index> <bytes>\n");
		}
		const std::string file_id = parts[1];
		const std::uint32_t chunk_index = static_cast<std::uint32_t>(std::stoul(parts[2]));
		const std::size_t bytes = static_cast<std::size_t>(std::stoull(parts[3]));
		std::vector<char> data;
		if (!read_exact(fd, data, bytes)) {
			return false;
		}
		if (!store_.store_chunk(file_id, chunk_index, data)) {
			return write_all(fd, "ERR store_failed\n");
		}
		return write_all(fd, "OK\n");
	}
	if (parts[0] == "GET_CHUNK") {
		if (parts.size() != 3) {
			return write_all(fd, "ERR usage GET_CHUNK <file_id> <chunk_index>\n");
		}
		const std::string file_id = parts[1];
		const std::uint32_t chunk_index = static_cast<std::uint32_t>(std::stoul(parts[2]));
		const auto data = store_.get_chunk(file_id, chunk_index);
		if (!data.has_value()) {
			return write_all(fd, "ERR chunk_not_found\n");
		}
		std::ostringstream header;
		header << "CHUNK_DATA " << data->size() << "\n";
		return write_all(fd, header.str()) && write_all(fd, data->data(), data->size());
	}

	return write_all(fd, "ERR unknown_command\n");
}

void storage_node::handle_client(int fd) {
	while (true) {
		const auto line = read_line(fd);
		if (!line.has_value()) {
			break;
		}
		if (!handle_command(fd, *line)) {
			break;
		}
	}
	close(fd);
}
