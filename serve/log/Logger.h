#ifndef LOGGER_H
#define LOGGER_H

#include <string>
#include <fstream>
#include <mutex>
#include <memory>
#include <thread>
// #include <sstream> // No longer needed
// #include <utility> // No longer needed for std::forward in log()
#include <atomic>
#include <cstdarg> // For va_list, vsnprintf
#include <cstdio>  // For FILE*, fwrite (if using C style IO)

// Forward declaration
struct LogBuffer;

class Logger {
public:
    explicit Logger(const std::string& log_filename, bool append = true);
    ~Logger();

    // --- Non-copyable and Movable ---
    Logger(const Logger&) = delete;
    Logger& operator=(const Logger&) = delete;
    Logger(Logger&&) = default;
    Logger& operator=(Logger&&) = default;

    // --- Optimized Variadic Log Function (Printf-style) ---
    /**
     * @brief Logs a message using printf-style formatting for high performance.
     * WARNING: Not type-safe! Format specifiers must match argument types.
     * Example: logger.log("Processing %d items for user %s", 10, user_name);
     * @param fmt The C-style format string.
     * @param args Arguments corresponding to the format specifiers.
     */
    void log(const char* fmt, ...) const; // Made const if possible

    // --- Console Logging Control ---
    void set_log_to_console(bool enable);

private:
    // --- Core logging function (accepts pre-formatted C-string) ---
    /**
     * @brief Internal function to log a pre-formatted C-string.
     * @param formatted_message The message already formatted in a buffer.
     * @param message_len The length of the formatted message.
     */
    void log_cstring(const char* formatted_message, size_t message_len) const; // Made const

    // --- Helper for timestamp ---
    /**
     * @brief Gets the current timestamp formatted into a thread-local buffer.
     * @param buffer A buffer to write the timestamp into.
     * @param buffer_size The size of the buffer.
     * @return The number of characters written (excluding null terminator), or 0 on error.
     */
    size_t getCurrentTimestamp(char* buffer, size_t buffer_size) const; // Made const

    // --- Member Variables ---
    // Use mutable if log() and log_cstring() are const, to allow locking
    mutable std::ofstream log_file_;
    mutable std::mutex mutex_;
    std::atomic<bool> is_open_{false}; // Atomic for potentially lock-free read checks (though write needs lock)
    std::atomic<bool> log_to_console_{false};

    // --- Platform specific defines for thread-safe time functions ---
    #if defined(_WIN32) || defined(_WIN64)
        #define LOCALTIME_S(tm_struct, time_t_val) localtime_s(tm_struct, time_t_val)
    #else // POSIX
        #define LOCALTIME_S(tm_struct, time_t_val) localtime_r(time_t_val, tm_struct)
    #endif
};

#endif // LOGGER_H