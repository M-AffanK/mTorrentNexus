#include "utils/socket_utils.hpp"

bool write_all(int fd, const void* data, std::size_t size) {
    const char* ptr = static_cast<const char*>(data);
    std::size_t sent = 0;
    while (sent < size) {
        const ssize_t n = send(fd, ptr + sent, size - sent, 0);
        if (n <= 0) {
            return false;
        }
        sent += static_cast<std::size_t>(n);
    }
    return true;
}

bool write_all(int fd, const std::string& text) {
    return write_all(fd, text.data(), text.size());
}

bool read_exact(int fd, std::vector<char>& out, std::size_t size) {
    out.assign(size, 0);
    std::size_t got = 0;
    while (got < size) {
        const ssize_t n = recv(fd, out.data() + got, size - got, 0);
        if (n <= 0) {
            return false;
        }
        got += static_cast<std::size_t>(n);
    }
    return true;
}

std::optional<std::string> read_line(int fd) {
    std::string line;
    char c = 0;
    while (true) {
        const ssize_t n = recv(fd, &c, 1, 0);
        if (n <= 0) {
            return std::nullopt;
        }
        if (c == '\n') {
            break;
        }
        if (c != '\r') {
            line.push_back(c);
        }
    }
    return line;
}

int connect_tcp(const std::string& host, uint16_t port) {
    struct addrinfo hints;
    std::memset(&hints, 0, sizeof(hints));
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;

    struct addrinfo* result = nullptr;
    const std::string service = std::to_string(port);
    if (getaddrinfo(host.c_str(), service.c_str(), &hints, &result) != 0) {
        return -1;
    }

    int fd = -1;
    for (struct addrinfo* rp = result; rp != nullptr; rp = rp->ai_next) {
        fd = socket(rp->ai_family, rp->ai_socktype, rp->ai_protocol);
        if (fd == -1) {
            continue;
        }
        if (connect(fd, rp->ai_addr, rp->ai_addrlen) == 0) {
            break;
        }
        close(fd);
        fd = -1;
    }

    freeaddrinfo(result);
    return fd;
}
