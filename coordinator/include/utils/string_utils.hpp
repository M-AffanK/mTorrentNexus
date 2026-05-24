#ifndef STRING_UTILS_HPP
#define STRING_UTILS_HPP

#include <cstddef>
#include <sstream>
#include <string>
#include <vector>
#include <cstring>

std::vector<std::string> split(const std::string& text, char delimiter = ' ');
std::string trim(const std::string& s);
std::string to_lower(std::string s);
std::string escape_field(const std::string& input);
std::string unescape_field(const std::string& input);

#endif // STRING_UTILS_HPP
