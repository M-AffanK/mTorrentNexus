#include "utils/string_utils.hpp"

std::vector<std::string> split(const std::string& text, char delimiter) {
    std::vector<std::string> parts;
    std::stringstream ss(text);
    std::string item;
    while (std::getline(ss, item, delimiter)) {
        if (!item.empty()) {
            parts.push_back(item);
        }
    }
    return parts;
}

std::string trim(const std::string& s) {
    std::size_t start = 0;
    while (start < s.size() && std::isspace(static_cast<unsigned char>(s[start])) != 0) {
        ++start;
    }
    std::size_t end = s.size();
    while (end > start && std::isspace(static_cast<unsigned char>(s[end - 1])) != 0) {
        --end;
    }
    return s.substr(start, end - start);
}

std::string to_lower(std::string s) {
    for (char& c : s) {
        c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
    }
    return s;
}

std::string escape_field(const std::string& input) {
    std::string out;
    out.reserve(input.size() + 8);
    for (const char c : input) {
        switch (c) {
            case '\\':
                out += "\\\\";
                break;
            case '\t':
                out += "\\t";
                break;
            case '\n':
                out += "\\n";
                break;
            default:
                out.push_back(c);
                break;
        }
    }
    return out;
}

std::string unescape_field(const std::string& input) {
    std::string out;
    out.reserve(input.size());
    for (std::size_t i = 0; i < input.size(); ++i) {
        if (input[i] == '\\' && i + 1 < input.size()) {
            const char next = input[i + 1];
            if (next == 't') {
                out.push_back('\t');
                ++i;
                continue;
            }
            if (next == 'n') {
                out.push_back('\n');
                ++i;
                continue;
            }
            if (next == '\\') {
                out.push_back('\\');
                ++i;
                continue;
            }
        }
        out.push_back(input[i]);
    }
    return out;
}
