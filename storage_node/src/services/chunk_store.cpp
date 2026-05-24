#include "services/chunk_store.hpp"

chunk_store::chunk_store(std::string root_dir) : root_dir_(std::move(root_dir)) {
	std::filesystem::create_directories(root_dir_);
}

bool chunk_store::store_chunk(const std::string& file_id, std::uint32_t chunk_index, const std::vector<char>& data) {
	std::scoped_lock<std::mutex> lock(mu_);
	const auto path = std::filesystem::path(root_dir_) / chunk_file_name(file_id, chunk_index);
	std::ofstream out(path, std::ios::binary | std::ios::trunc);
	if (!out.is_open()) {
		return false;
	}
	out.write(data.data(), static_cast<std::streamsize>(data.size()));
	return out.good();
}

std::optional<std::vector<char>> chunk_store::get_chunk(const std::string& file_id, std::uint32_t chunk_index) const {
	std::scoped_lock<std::mutex> lock(mu_);
	const auto path = std::filesystem::path(root_dir_) / chunk_file_name(file_id, chunk_index);
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

std::string chunk_store::chunk_file_name(const std::string& file_id, std::uint32_t chunk_index) const {
	std::ostringstream out;
	out << file_id << "_" << chunk_index << ".chunk";
	return out.str();
}
