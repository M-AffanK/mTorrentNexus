#include "server/storage_node.hpp"

int main(int argc, char** argv) {
    if (argc < 8) {
        std::cerr << "Usage:\n"
        << "  mtorrent_storage_node <node_id> <listen_port> <storage_dir> "
        << "<coordinator_host> <coordinator_port> <advertised_host> <capacity_bytes>\n";
        return EXIT_FAILURE;
    }

    const std::string node_id = argv[1];
    const uint16_t listen_port = static_cast<uint16_t>(std::stoi(argv[2]));
    const std::string storage_dir = argv[3];
    const std::string coordinator_host = argv[4];
    const uint16_t coordinator_port = static_cast<uint16_t>(std::stoi(argv[5]));
    const std::string advertised_host = argv[6];
    const std::uint64_t capacity_bytes = std::stoull(argv[7]);

    storage_node node(listen_port, node_id, advertised_host, storage_dir);

    if (!node.register_with_coordinator(coordinator_host, coordinator_port, capacity_bytes)) {
        std::cerr << "Failed to register with coordinator\n";
        return EXIT_FAILURE;
    }
    std::cout << "Node registered with coordinator\n";

    if (!node.run()) {
        std::cerr << "Failed to start storage node server\n";
        return EXIT_FAILURE;
    }
    return EXIT_SUCCESS;
}
