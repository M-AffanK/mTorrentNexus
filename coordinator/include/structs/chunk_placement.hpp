#ifndef CHUNK_PLACEMENT_HPP
#define CHUNK_PLACEMENT_HPP

#include <cstdint>
#include <vector>
#include <string>

struct chunk_placement {
    std::uint32_t index = 0;
    std::uint64_t size = 0;
    std::vector<std::string> node_ids;
};

#endif // CHUNK_PLACEMENT_HPP
