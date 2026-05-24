#ifndef SOCKET_UTILS_HPP
#define SOCKET_UTILS_HPP

#include <cstdint>
#include <cstddef>
#include <string>
#include <vector>
#include <optional>
#include <cstring>

#include <arpa/inet.h>
#include <netdb.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>

bool write_all(int fd, const void* data, std::size_t size);
bool write_all(int fd, const std::string& text);
bool read_exact(int fd, std::vector<char>& out, std::size_t size);
std::optional<std::string> read_line(int fd);
int connect_tcp(const std::string& host, uint16_t port);

#endif // SOCKET_UTILS_HPP
