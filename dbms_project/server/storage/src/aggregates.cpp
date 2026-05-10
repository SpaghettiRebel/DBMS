#include "aggregates.h"
#include <stdexcept>
#include <cmath>

namespace dbms {

// Реализация методов AggregateResult вынесена в заголовок для простоты (inline)
// Этот файл может использоваться для дополнительных утилитных функций агрегации

std::string aggregate_type_to_string(AggregateType type) {
    switch (type) {
        case AggregateType::COUNT: return "COUNT";
        case AggregateType::SUM: return "SUM";
        case AggregateType::AVG: return "AVG";
        default: return "NONE";
    }
}

bool is_aggregate_function(const std::string& func_name) {
    std::string upper_name = func_name;
    for (auto& c : upper_name) c = toupper(c);
    return (upper_name == "COUNT" || upper_name == "SUM" || upper_name == "AVG");
}

AggregateType parse_aggregate_type(const std::string& func_name) {
    std::string upper_name = func_name;
    for (auto& c : upper_name) c = toupper(c);
    
    if (upper_name == "COUNT") return AggregateType::COUNT;
    if (upper_name == "SUM") return AggregateType::SUM;
    if (upper_name == "AVG") return AggregateType::AVG;
    
    return AggregateType::NONE;
}

} // namespace dbms
