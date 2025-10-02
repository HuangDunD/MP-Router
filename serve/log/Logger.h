// log_sys.h
#ifndef LOG_SYS_H
#define LOG_SYS_H

#include <iostream>
#include <fstream>
#include <string>
#include <vector>
#include <chrono>
#include <iomanip>
#include <ctime>
#include <sstream> // For std::stringstream (our buffer)
#include <mutex>
#include <atomic>

inline void formatCurrentTime(char* buffer, size_t buffer_size) {
    using namespace std::chrono;

    auto now = system_clock::now();
    auto in_time_t = system_clock::to_time_t(now);
    auto duration = now.time_since_epoch();
    auto ms = duration_cast<milliseconds>(duration) % 1000;

    std::tm timeinfo{};
    localtime_r(&in_time_t, &timeinfo); // POSIX


    // Format: YYYY-MM-DD HH:MM:SS.mmm
    std::snprintf(buffer, buffer_size, "%04d-%02d-%02d %02d:%02d:%02d.%03d",
        timeinfo.tm_year + 1900,
        timeinfo.tm_mon + 1,
        timeinfo.tm_mday,
        timeinfo.tm_hour,
        timeinfo.tm_min,
        timeinfo.tm_sec,
        static_cast<int>(ms.count()));
}

class Logger {
public:
    enum class LogLevel { DEBUG, INFO, WARNING, ERROR };
    enum class LogTarget { FILE_ONLY, TERMINAL_ONLY, FILE_AND_TERMINAL };

private:
    std::ofstream outfile;
    LogTarget target;
    std::string path;
    LogLevel min_level;
    // Synchronization for multi-threaded logging
    mutable std::mutex mutex_;
    std::atomic<bool> shutting_down_{false};

    // Buffering members for file output
    std::stringstream file_log_buffer;
    size_t buffer_capacity_bytes; // Approx. capacity in bytes

    void output(const std::string& text, LogLevel current_message_level);

    static const char* levelToString(LogLevel level) {
        switch (level) {
            case LogLevel::DEBUG:   return "[DEBUG]   ";
            case LogLevel::INFO:    return "[INFO]    ";
            case LogLevel::WARNING: return "[WARNING] ";
            case LogLevel::ERROR:   return "[ERROR]   ";
            default:                return "[UNKNOWN] ";
        }
    }

    // Helper to flush the file buffer. Caller MUST hold mutex_.
    void flush_file_buffer() {
        if (!outfile.is_open() || file_log_buffer.tellp() == 0) { // No file or buffer empty
            return;
        }
        // Efficiently write stringstream's internal buffer to the file stream
        outfile << file_log_buffer.rdbuf();
        outfile.flush(); // IMPORTANT: Ensure data is written to disk
        file_log_buffer.str("");    // Clear the contents of the stringstream
        file_log_buffer.clear();    // Clear error flags (like EOF) and reset stream state
    }

public:
    // Default buffer capacity can be a static const or a default argument
    static const size_t DEFAULT_BUFFER_CAPACITY = 4096; // 4KB

    Logger();
    Logger(LogTarget target,
           LogLevel min_level,
           const std::string& file_path = "",
           size_t buffer_cap_bytes = DEFAULT_BUFFER_CAPACITY);
    ~Logger();

    void debug(const std::string& text);
    void info(const std::string& text);
    void warning(const std::string& text);
    void error(const std::string& text);

    void setMinLevel(LogLevel new_level);
    void flush(); // Public method to manually flush buffers
};

#endif // LOG_SYS_H

