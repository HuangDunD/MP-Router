#pragma once
#include <atomic>
#include <cstddef>
#include <fstream>
#include <mutex>

class Logger {
public:
    explicit Logger(const std::string& filename, bool append = true);
    ~Logger();

    void set_log_to_console(bool enable);
    void log(const char* fmt, ...) const; // printf style

private:
    void log_cstring(const char* msg, size_t len) const;

    mutable std::mutex      mutex_;
    mutable std::ofstream   log_file_;
    std::atomic_bool        is_open_{false};
    std::atomic_bool        log_to_console_{false};
};
