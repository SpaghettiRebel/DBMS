
#include <stdexcept>
#include <string>

class not_implemented : public std::logic_error {
public:
    not_implemented(const std::string& msg, const std::string& detail) : std::logic_error(msg + ": " + detail) {}
};