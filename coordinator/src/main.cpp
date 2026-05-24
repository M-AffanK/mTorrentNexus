#include "server/coordinator.hpp"

int main(int argc, char** argv) {
    uint16_t port = 9090;
    std::size_t replication = 2;
    std::string metadata_path = "server_metadata.db";

    if (argc > 1) {
        port = static_cast<uint16_t>(std::stoi(argv[1]));
    }
    if (argc > 2) {
        replication = static_cast<std::size_t>(std::stoul(argv[2]));
    }
    if (argc > 3) {
        metadata_path = argv[3];
    }

    coordinator server(port, replication, metadata_path);
    if (!server.run()) {
        std::cerr << "Failed to start coordinator server\n";
        return EXIT_FAILURE;
    }
    return EXIT_SUCCESS;
}
