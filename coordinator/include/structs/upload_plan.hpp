#ifndef UPLOAD_PLAN_HPP
#define UPLOAD_PLAN_HPP

#include <cstdint>
#include <vector>
#include <string>
#include <set>

#include "chunk_placement.hpp"

struct upload_plan {
    std::string upload_id;
    std::string file_id;
    std::string file_name;
    std::uint64_t file_size = 0;
    std::uint32_t chunk_size = 0;
    std::uint32_t chunk_count = 0;
    std::vector<chunk_placement> chunks;
    std::set<std::uint32_t> uploaded_indices;
};

#endif // UPLOAD_PLAN_HPP
