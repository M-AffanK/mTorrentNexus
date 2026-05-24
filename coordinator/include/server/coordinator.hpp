#ifndef COORDINATOR_HPP
#define COORDINATOR_HPP

#include <iostream>
#include <thread>
#include <sstream>

#include "utils/socket_utils.hpp"
#include "services/coordinator_service.hpp"

class coordinator {
public:
	coordinator(uint16_t port, std::size_t replication_factor, std::string metadata_path);
	bool run();

private:
	bool stream_file_to_client(int fd, const file_record& file);
	bool store_chunk_on_node(const storage_node& node, const std::string& file_id,
						 std::uint32_t chunk_index, const std::vector<char>& payload);
	std::optional<std::vector<char>> fetch_chunk_from_node(const storage_node& node,
													const std::string& file_id, std::uint32_t chunk_index);
	bool handle_command(int fd, const std::string& line);
	void handle_client(int fd);
private:
	uint16_t port_;
	int listen_fd_ = -1;
	coordinator_service state_;
};

#endif // COORDINATOR_HPP
