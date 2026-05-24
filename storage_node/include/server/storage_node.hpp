#ifndef STORAGE_NODE_HPP
#define STORAGE_NODE_HPP

#include <cstdint>
#include <string>
#include <iostream>
#include <thread>

#include "services/chunk_store.hpp"
#include "utils/socket_utils.hpp"
#include "utils/string_utils.hpp"

class storage_node {
public:
	storage_node(uint16_t port, std::string node_id, std::string host, std::string storage_dir);
	bool register_with_coordinator(const std::string& coordinator_host,
			uint16_t coordinator_port, std::uint64_t capacity_bytes);
	bool run();

private:
    bool handle_command(int fd, const std::string& line);
    void handle_client(int fd);

private:
    uint16_t port_;
    std::string node_id_;
    std::string host_;
    chunk_store store_;
    int listen_fd_ = -1;
};

#endif // STORAGE_NODE_HPP
