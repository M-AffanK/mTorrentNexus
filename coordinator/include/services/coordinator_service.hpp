#ifndef COORDINATOR_SERVICE_HPP
#define COORDINATOR_SERVICE_HPP

#include <unordered_map>
#include <mutex>
#include <atomic>
#include <optional>
#include <fstream>
#include <filesystem>
#include <algorithm>

#include "structs/file_record.hpp"
#include "structs/storage_node.hpp"
#include "structs/chunk_placement.hpp"
#include "structs/upload_plan.hpp"
#include "utils/string_utils.hpp"

class coordinator_service {
public:
	coordinator_service(std::size_t replication_factor, std::string metadata_path);

	void
	register_node(const storage_node& node);

	std::optional<upload_plan>
	create_upload_plan(const std::string& file_name, std::uint64_t file_size,
						std::uint32_t chunk_size, std::uint32_t chunk_count);

	std::optional<upload_plan>
	get_upload(const std::string& upload_id) const;

	bool
	mark_chunk_uploaded(const std::string& upload_id, std::uint32_t chunk_index);

	std::optional<file_record>
	finalize_upload(const std::string& upload_id);

	std::optional<file_record>
	get_file(const std::string& file_id) const;

	std::optional<file_record>
	get_file_by_share_token(const std::string& share_token) const;

	std::vector<file_record>
	list_files() const;

	std::vector<file_record>
	search_files_by_name(const std::string& query) const;

	std::optional<std::string>
	share_url_for_file(const std::string& file_id) const;
	
	std::optional<storage_node>
	node_by_id(const std::string& id) const;
	
private:
	std::string
	next_id(const std::string& prefix);

	void
	load_metadata();

	void
	save_metadata_locked() const;

private:
	std::size_t replication_factor_;
    std::string metadata_path_;
    mutable std::mutex mu_;
    std::unordered_map<std::string, storage_node> nodes_;
    std::unordered_map<std::string, upload_plan> uploads_;
    std::unordered_map<std::string, file_record> files_;
    std::unordered_map<std::string, std::string> share_to_file_;
    std::atomic<std::uint64_t> counter_{0};
};

#endif // COORDINATOR_SERVICE_HPP
