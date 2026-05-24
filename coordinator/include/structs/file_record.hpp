#ifndef FILE_RECORD_HPP
#define FILE_RECORD_HPP

#include <cstdint>
#include <vector>
#include <string>

#include "chunk_placement.hpp"

struct file_record {
    std::string file_id;
    std::string file_name;
    std::uint64_t file_size = 0;
    std::uint32_t chunk_size = 0;
    std::uint32_t chunk_count = 0;
    std::string share_token;
    std::vector<chunk_placement> chunks;
};

#endif // FILE_RECORD_HPP
