#ifndef STORAGE_NODE_HPP
#define STORAGE_NODE_HPP

#include <cstdint>
#include <string>

struct storage_node {
    std::string id;
    std::string host;
    uint16_t port = 0;
    std::uint64_t capacity_bytes = 0;
};

#endif // STORAGE_NODE_HPP
