#include "server/coordinator.hpp"

//public
coordinator::coordinator(uint16_t port, std::size_t replication_factor, std::string metadata_path)
: port_(port), state_(replication_factor, std::move(metadata_path)) {}

bool coordinator::run() {
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

	std::cout << "Coordinator listening on " << port_ << "\n";
	while (true) {
		sockaddr_in client_addr;
		socklen_t len = sizeof(client_addr);
		int client_fd = accept(listen_fd_, reinterpret_cast<sockaddr*>(&client_addr), &len);
		if (client_fd < 0) {
			continue;
		}
		std::thread(&coordinator::handle_client, this, client_fd).detach();
	}
}

//private
bool coordinator::stream_file_to_client(int fd, const file_record& file) {
	std::ostringstream head;
	head << "OK " << file.file_name << " " << file.file_size << " "
	<< file.chunk_size << " " << file.chunk_count << "\n"; // << file.file_id << " "
	if (!write_all(fd, head.str())) {
		return false;
	}

	for (const auto& chunk : file.chunks) {
		std::optional<std::vector<char>> data;
		for (const auto& node_id : chunk.node_ids) {
			const auto node = state_.node_by_id(node_id);
			if (!node.has_value()) {
				continue;
			}
			data = fetch_chunk_from_node(*node, file.file_id, chunk.index);
			if (data.has_value()) {
				break;
			}
		}
		if (!data.has_value()) {
			return write_all(fd, "ERR missing_chunk\n");
		}
		std::ostringstream meta;
		meta << "CHUNK " << chunk.index << " " << data->size() << "\n";
		if (!write_all(fd, meta.str()) || !write_all(fd, data->data(), data->size())) {
			return false;
		}
	}
	return write_all(fd, "END_FILE\n");
}

bool coordinator::store_chunk_on_node(const storage_node& node,
						 const std::string& file_id,
						 std::uint32_t chunk_index,
						 const std::vector<char>& payload) {
	int fd = connect_tcp(node.host, node.port);
	if (fd < 0) {
		return false;
	}
	std::ostringstream command;
	command << "STORE_CHUNK " << file_id << " " << chunk_index << " " << payload.size() << "\n";
	if (!write_all(fd, command.str()) || !write_all(fd, payload.data(), payload.size())) {
		close(fd);
		return false;
	}
	const auto ack = read_line(fd);
	close(fd);
	return ack.has_value() && ack.value() == "OK";
 }

std::optional<std::vector<char>> coordinator::fetch_chunk_from_node(const storage_node& node,
													const std::string& file_id,
													std::uint32_t chunk_index) {
	 int fd = connect_tcp(node.host, node.port);
	 if (fd < 0) {
		 return std::nullopt;
	 }
	 std::ostringstream command;
	 command << "GET_CHUNK " << file_id << " " << chunk_index << "\n";
	 if (!write_all(fd, command.str())) {
		 close(fd);
		 return std::nullopt;
	 }
	 const auto meta = read_line(fd);
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
	 if (!read_exact(fd, data, size)) {
		 close(fd);
		 return std::nullopt;
	 }
	 close(fd);
	 return data;
}

bool coordinator::handle_command(int fd, const std::string& line) {
	const auto parts = split(line);
	if (parts.empty()) {
		return true;
	}
	const std::string& cmd = parts[0];

	if (cmd == "PING") {
		return write_all(fd, "PONG\n");
	}
	if (cmd == "REGISTER_NODE") {
		if (parts.size() != 5) {
			return write_all(fd, "ERR usage REGISTER_NODE <id> <host> <port> <capacity>\n");
		}
		storage_node node;
		node.id = parts[1];
		node.host = parts[2];
		node.port = static_cast<uint16_t>(std::stoi(parts[3]));
		node.capacity_bytes = std::stoull(parts[4]);
		state_.register_node(node);
		return write_all(fd, "OK node_registered\n");
	}
	if (cmd == "UPLOAD_INIT") {
		if (parts.size() != 5) {
			return write_all(fd, "ERR usage UPLOAD_INIT <filename> <filesize> <chunksize> <chunkcount>\n");
		}
		const auto plan = state_.create_upload_plan(parts[1], std::stoull(parts[2]),
													static_cast<std::uint32_t>(std::stoul(parts[3])),
													static_cast<std::uint32_t>(std::stoul(parts[4])));
		if (!plan.has_value()) {
			return write_all(fd, "ERR no_storage_nodes\n");
		}

		std::ostringstream out;
		out << "OK " << plan->upload_id << " " << plan->file_id << "\n";
		for (const auto& chunk : plan->chunks) {
			out << "CHUNK " << chunk.index << " " << chunk.node_ids.size();
			for (const auto& node_id : chunk.node_ids) {
				out << " " << node_id;
			}
			out << "\n";
		}
		out << "END_PLAN\n";
		return write_all(fd, out.str());
	}
	if (cmd == "UPLOAD_CHUNK") {
		if (parts.size() != 4) {
			return write_all(fd, "ERR usage UPLOAD_CHUNK <upload_id> <chunk_index> <bytes>\n");
		}
		const std::string upload_id = parts[1];
		const std::uint32_t chunk_index = static_cast<std::uint32_t>(std::stoul(parts[2]));
		const std::size_t bytes = static_cast<std::size_t>(std::stoull(parts[3]));
		std::vector<char> payload;
		if (!read_exact(fd, payload, bytes)) {
			return false;
		}

		const auto plan = state_.get_upload(upload_id);
		if (!plan.has_value() || chunk_index >= plan->chunks.size()) {
			return write_all(fd, "ERR invalid_upload_or_chunk\n");
		}

		std::size_t success = 0;
		for (const auto& node_id : plan->chunks[chunk_index].node_ids) {
			const auto node = state_.node_by_id(node_id);
			if (node.has_value() && store_chunk_on_node(*node, plan->file_id, chunk_index, payload)) {
				++success;
			}
		}
		if (success == 0) {
			return write_all(fd, "ERR chunk_store_failed\n");
		}
		state_.mark_chunk_uploaded(upload_id, chunk_index);
		return write_all(fd, "OK chunk_stored\n");
	}
	if (cmd == "UPLOAD_FINISH") {
		if (parts.size() != 2) {
			return write_all(fd, "ERR usage UPLOAD_FINISH <upload_id>\n");
		}
		const auto file = state_.finalize_upload(parts[1]);
		if (!file.has_value()) {
			return write_all(fd, "ERR upload_incomplete_or_missing\n");
		}
		std::ostringstream out;
		out << "OK " << file->file_id << " mtorrent://share/" << file->share_token << "\n";
		return write_all(fd, out.str());
	}
	if (cmd == "DOWNLOAD") {
		if (parts.size() != 2) {
			return write_all(fd, "ERR usage DOWNLOAD <file_id>\n");
		}
		const auto file = state_.get_file(parts[1]);
		if (!file.has_value()) {
			return write_all(fd, "ERR file_not_found\n");
		}
		return stream_file_to_client(fd, *file);
	}
	if (cmd == "DOWNLOAD_SHARE") {
		if (parts.size() != 2) {
			return write_all(fd, "ERR usage DOWNLOAD_SHARE <share_token>\n");
		}
		std::string token = parts[1];
		const std::string prefix = "mtorrent://share/";
		if (token.rfind(prefix, 0) == 0) {
			token = token.substr(prefix.size());
		}
		const auto file = state_.get_file_by_share_token(token);
		if (!file.has_value()) {
			return write_all(fd, "ERR share_not_found\n");
		}
		return stream_file_to_client(fd, *file);
	}
	if (cmd == "LIST_FILES") {
		const auto files = state_.list_files();
		std::ostringstream out;
		out << "OK " << files.size() << "\n";
		for (const auto& file : files) {
			out << "FILE " << file.file_id << " " << file.file_name << " "
			<< file.file_size << " " << file.chunk_count << " "
			<< "mtorrent://share/" << file.share_token << "\n";
		}
		out << "END_LIST\n";
		return write_all(fd, out.str());
	}
	if (cmd == "SEARCH_FILES") {
		std::string query;
		if (line.size() > std::string("SEARCH_FILES").size()) {
			query = trim(line.substr(std::string("SEARCH_FILES").size()));
		}
		if (query.empty()) {
			return write_all(fd, "ERR usage SEARCH_FILES <name-substring>\n");
		}
		const auto files = state_.search_files_by_name(query);
		std::ostringstream out;
		out << "OK " << files.size() << "\n";
		for (const auto& file : files) {
			out << "FILE " << file.file_id << " " << file.file_name << " "
			<< file.file_size << " " << file.chunk_count << " "
			<< "mtorrent://share/" << file.share_token << "\n";
		}
		out << "END_SEARCH\n";
		return write_all(fd, out.str());
	}
	if (cmd == "GET_SHARE_LINK") {
		if (parts.size() != 2) {
			return write_all(fd, "ERR usage GET_SHARE_LINK <file_id>\n");
		}
		const auto url = state_.share_url_for_file(parts[1]);
		if (!url.has_value()) {
			return write_all(fd, "ERR file_not_found\n");
		}
		std::ostringstream out;
		out << "OK " << *url << "\n";
		return write_all(fd, out.str());
	}

	return write_all(fd, "ERR unknown_command\n");
}

void coordinator::handle_client(int fd) {
	try {
		while (true) {
			const auto line = read_line(fd);
			if (!line.has_value()) {
				break;
			}
			if (!handle_command(fd, *line)) {
				break;
			}
		}
	} catch (const std::exception& ex) {
		std::cerr << "[session] error: " << ex.what() << "\n";
	}
	close(fd);
}
