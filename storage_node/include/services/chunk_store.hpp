#ifndef CHUNK_STORE_HPP
#define CHUNK_STORE_HPP

#include <string>
#include <vector>
#include <optional>
#include <cstdint>
#include <mutex>
#include <filesystem>
#include <fstream>

class chunk_store {
public:
	explicit chunk_store(std::string root_dir);
	bool store_chunk(const std::string& file_id, std::uint32_t chunk_index, const std::vector<char>& data);
	std::optional<std::vector<char>> get_chunk(const std::string& file_id, std::uint32_t chunk_index) const;

private:
	std::string chunk_file_name(const std::string& file_id, std::uint32_t chunk_index) const;

private:
	std::string root_dir_;
    mutable std::mutex mu_;
};

#endif // CHUNK_STORE_HPP
