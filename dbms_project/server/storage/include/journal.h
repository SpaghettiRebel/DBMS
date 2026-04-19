#pragma once
#include <string>
#include <vector>
#include <fstream>
#include "../shared/QueryPlan.h"

enum class JournalOp { INSERT, UPDATE, DELETE };

struct JournalEntry {
    JournalOp op;
    std::string table_name;
    std::string timestamp;
    std::vector<char> old_data; // Для UPDATE и DELETE
    std::vector<char> new_data; // Для INSERT и UPDATE
};

class Journal {
private:
    std::string journal_path;
    std::fstream file;

public:
    explicit Journal(const std::string& path);
    ~Journal();

    void log(const JournalEntry& entry);
    std::vector<JournalEntry> get_all_entries();
};
