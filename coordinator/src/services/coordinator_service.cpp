#include "services/coordinator_service.hpp"

//public
coordinator_service::coordinator_service(std::size_t replication_factor, std::string metadata_path)
: replication_factor_(replication_factor == 0 ? 1 : replication_factor), metadata_path_(std::move(metadata_path)) {
	load_metadata();
}

void coordinator_service::register_node(const storage_node& node) {
	std::scoped_lock<std::mutex> lock(mu_);
	nodes_[node.id] = node;
}

std::optional<upload_plan> coordinator_service::create_upload_plan(const std::string& file_name,
											  std::uint64_t file_size,
											  std::uint32_t chunk_size,
											  std::uint32_t chunk_count) {
	std::scoped_lock<std::mutex> lock(mu_);
	if (nodes_.empty()) {
		return std::nullopt;
	}

	upload_plan plan;
	plan.upload_id = next_id("upl");
	plan.file_id = next_id("file");
	plan.file_name = file_name;
	plan.file_size = file_size;
	plan.chunk_size = chunk_size;
	plan.chunk_count = chunk_count;

	std::vector<std::string> node_order;
	node_order.reserve(nodes_.size());
	for (const auto& [id, _] : nodes_) {
		node_order.push_back(id);
	}
	std::sort(node_order.begin(), node_order.end());

	const std::size_t replicas = std::min(replication_factor_, node_order.size());
	for (std::uint32_t i = 0; i < chunk_count; ++i) {
		chunk_placement placement;
		placement.index = i;
		const std::uint64_t offset = static_cast<std::uint64_t>(i) * chunk_size;
		placement.size = std::min<std::uint64_t>(chunk_size, file_size - offset);
		for (std::size_t r = 0; r < replicas; ++r) {
			placement.node_ids.push_back(node_order[(i + r) % node_order.size()]);
		}
		plan.chunks.push_back(placement);
	}

	uploads_[plan.upload_id] = plan;
	return plan;
}

std::optional<upload_plan> coordinator_service::get_upload(const std::string& upload_id) const {
	std::scoped_lock<std::mutex> lock(mu_);
	const auto it = uploads_.find(upload_id);
	if (it == uploads_.end()) {
	  return std::nullopt;
	}
	return it->second;
}

bool coordinator_service::mark_chunk_uploaded(const std::string& upload_id, std::uint32_t chunk_index) {
	std::scoped_lock<std::mutex> lock(mu_);
	auto it = uploads_.find(upload_id);
	if (it == uploads_.end() || chunk_index >= it->second.chunk_count) {
	  return false;
	}
	it->second.uploaded_indices.insert(chunk_index);
	return true;
}

std::optional<file_record> coordinator_service::finalize_upload(const std::string& upload_id) {
	std::scoped_lock<std::mutex> lock(mu_);
	auto it = uploads_.find(upload_id);
	if (it == uploads_.end()) {
	  return std::nullopt;
	}
	if (it->second.uploaded_indices.size() != it->second.chunk_count) {
	  return std::nullopt;
	}

	file_record file;
	file.file_id = it->second.file_id;
	file.file_name = it->second.file_name;
	file.file_size = it->second.file_size;
	file.chunk_size = it->second.chunk_size;
	file.chunk_count = it->second.chunk_count;
	file.share_token = next_id("shr");
	file.chunks = it->second.chunks;

	files_[file.file_id] = file;
	share_to_file_[file.share_token] = file.file_id;
	uploads_.erase(it);
	save_metadata_locked();
	return file;
}

std::optional<file_record> coordinator_service::get_file(const std::string& file_id) const {
	std::scoped_lock<std::mutex> lock(mu_);
	const auto it = files_.find(file_id);
	if (it == files_.end()) {
	  return std::nullopt;
	}
	return it->second;
}

std::optional<file_record> coordinator_service::get_file_by_share_token(const std::string& share_token) const {
	std::scoped_lock<std::mutex> lock(mu_);
	const auto it = share_to_file_.find(share_token);
	if (it == share_to_file_.end()) {
	  return std::nullopt;
	}
	const auto fit = files_.find(it->second);
	if (fit == files_.end()) {
	  return std::nullopt;
	}
	return fit->second;
}

std::vector<file_record> coordinator_service::list_files() const {
	std::scoped_lock<std::mutex> lock(mu_);
	std::vector<file_record> out;
	out.reserve(files_.size());
	for (const auto& [_, file] : files_) {
	  out.push_back(file);
	}
	return out;
}

std::vector<file_record> coordinator_service::search_files_by_name(const std::string& query) const {
	std::scoped_lock<std::mutex> lock(mu_);
	const std::string q = to_lower(query);
	std::vector<file_record> out;
	for (const auto& [_, file] : files_) {
	  if (to_lower(file.file_name).find(q) != std::string::npos) {
		  out.push_back(file);
	  }
	}
	return out;
}

std::optional<std::string> coordinator_service::share_url_for_file(const std::string& file_id) const {
	std::scoped_lock<std::mutex> lock(mu_);
	const auto it = files_.find(file_id);
	if (it == files_.end()) {
	  return std::nullopt;
	}
	return "mtorrent://share/" + it->second.share_token;
}

std::optional<storage_node> coordinator_service::node_by_id(const std::string& id) const {
	std::scoped_lock<std::mutex> lock(mu_);
	const auto it = nodes_.find(id);
	if (it == nodes_.end()) {
	  return std::nullopt;
	}
	return it->second;
}

//private
std::string coordinator_service::next_id(const std::string& prefix) {
	std::ostringstream os;
	os << prefix << "_" << ++counter_;
	return os.str();
}

void coordinator_service::load_metadata() {
	std::scoped_lock<std::mutex> lock(mu_);
	files_.clear();
	share_to_file_.clear();

	if (!std::filesystem::exists(metadata_path_)) {
		return;
	}

	std::ifstream in(metadata_path_);
	if (!in.is_open()) {
		return;
	}

	std::string line;
	while (std::getline(in, line)) {
		if (line.empty()) {
			continue;
		}
		if (line.rfind("COUNTER\t", 0) == 0) {
			const auto fields = split(line, '\t');
			if (fields.size() >= 2) {
				counter_ = std::stoull(fields[1]);
			}
			continue;
		}
		if (line.rfind("FILE\t", 0) == 0) {
			const auto fields = split(line, '\t');
			if (fields.size() < 7) {
				continue;
			}
			file_record file;
			file.file_id = fields[1];
			file.file_name = unescape_field(fields[2]);
			file.file_size = std::stoull(fields[3]);
			file.chunk_size = static_cast<std::uint32_t>(std::stoul(fields[4]));
			file.chunk_count = static_cast<std::uint32_t>(std::stoul(fields[5]));
			file.share_token = fields[6];

			while (std::getline(in, line)) {
				if (line == "ENDFILE") {
					break;
				}
				if (line.rfind("CHUNK\t", 0) != 0) {
					continue;
				}
				const auto cf = split(line, '\t');
				if (cf.size() < 5 || cf[1] != file.file_id) {
					continue;
				}
				chunk_placement chunk;
				chunk.index = static_cast<std::uint32_t>(std::stoul(cf[2]));
				chunk.size = std::stoull(cf[3]);
				chunk.node_ids = split(cf[4], ',');
				file.chunks.push_back(chunk);
			}

			files_[file.file_id] = file;
			if (!file.share_token.empty()) {
				share_to_file_[file.share_token] = file.file_id;
			}
		}
	}
}

void coordinator_service::save_metadata_locked() const {
	std::ofstream out(metadata_path_, std::ios::trunc);
	if (!out.is_open()) {
		return;
	}

	out << "COUNTER\t" << counter_.load() << "\n";
	for (const auto& [_, file] : files_) {
		out << "FILE\t" << file.file_id << "\t" << escape_field(file.file_name) << "\t"
		<< file.file_size << "\t" << file.chunk_size << "\t" << file.chunk_count
		<< "\t" << file.share_token << "\n";
		for (const auto& chunk : file.chunks) {
			out << "CHUNK\t" << file.file_id << "\t" << chunk.index << "\t" << chunk.size << "\t";
			for (std::size_t i = 0; i < chunk.node_ids.size(); ++i) {
				if (i > 0) {
					out << ",";
				}
				out << chunk.node_ids[i];
			}
			out << "\n";
		}
		out << "ENDFILE\n";
	}
}
