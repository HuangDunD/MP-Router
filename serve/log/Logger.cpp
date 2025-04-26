#include "Logger.h"
#include <iostream> // Needed for std::cerr, std::endl (only for critical errors now)
#include <chrono>
#include <stdexcept>
#include <thread>
#include <functional> // For std::hash
#include <ctime>      // For time_t, tm, strftime, localtime_r/s
#include <cstdio>     // For snprintf, vsnprintf, FILE*, fwrite
#include <cstdarg>    // For va_list, va_start, va_end
#include <vector>     // Only if dynamic buffer allocation is needed as fallback

// --- Configuration ---
constexpr size_t LOG_BUFFER_SIZE = 2048; // Adjust size as needed

// --- Thread-local storage for formatting buffers ---
// Ensures each thread has its own buffer, avoiding heap allocs and data races.
namespace { // Use anonymous namespace for internal linkage
    thread_local char g_format_buffer[LOG_BUFFER_SIZE];
    thread_local char g_entry_buffer[LOG_BUFFER_SIZE + 128]; // Extra space for timestamp, thread ID etc.
}

Logger::Logger(const std::string &log_filename, bool append) {
    std::ios_base::openmode mode = std::ios_base::out | std::ios_base::binary; // Use binary for potentially faster raw write
    if (append) {
        mode |= std::ios_base::app;
    } else {
        mode |= std::ios_base::trunc;
    }
    log_file_.open(log_filename, mode);
    if (!log_file_.is_open()) {
        // Use fprintf for critical startup stderr message to avoid complex dependencies
        fprintf(stderr, "Logger CRITICAL ERROR: Could not open log file: %s\n", log_filename.c_str());
        throw std::runtime_error("Logger Error: Could not open log file: " + log_filename);
    }
    is_open_.store(true); // Set atomic flag

    // Log initialization message (using the optimized C-style log)
    log("Logger initialized. Logging to file: %s", log_filename.c_str());
}

Logger::~Logger() {
    if (is_open_.load()) {
        log("Logger shutting down.");
        // ofstream destructor handles closing
    }
}

size_t Logger::getCurrentTimestamp(char* buffer, size_t buffer_size) const {
    if (buffer_size < 30) return 0; // Need sufficient space

    auto now = std::chrono::system_clock::now();
    auto now_c = std::chrono::system_clock::to_time_t(now);
    auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch()) % 1000;

    std::tm now_tm;
    // Use thread-safe time conversion
    if (LOCALTIME_S(&now_tm, &now_c) == nullptr ) { // Check POSIX return
         return 0; // Error
    }
     // Note: localtime_s on Windows returns errno_t, 0 on success. Check needed if strict.

    // Format date and time using strftime
    size_t len = std::strftime(buffer, buffer_size, "%Y-%m-%d %H:%M:%S", &now_tm);
    if (len == 0) return 0; // strftime failed

    // Append milliseconds using snprintf
    // Ensure there's space for ".XXX\0"
    if (len + 5 > buffer_size) return len; // Not enough space for ms

    int ms_len = snprintf(buffer + len, buffer_size - len, ".%03lld", static_cast<long long>(ms.count()));

    if (ms_len < 0) return len; // snprintf error

    return len + ms_len;
}


void Logger::set_log_to_console(bool enable) {
    log_to_console_.store(enable); // Set atomic flag
}

// --- High-performance printf-style log method ---
void Logger::log(const char* fmt, ...) const {
    va_list args;
    va_start(args, fmt);

    // Use vsnprintf to format the user message into the thread-local buffer
    // Returns the number of chars that *would* have been written (excluding null)
    // or negative on error.
    int message_len = vsnprintf(g_format_buffer, LOG_BUFFER_SIZE, fmt, args);

    va_end(args);

    if (message_len < 0) {
        // Formatting error, log a placeholder?
        fprintf(stderr, "Logger Error: vsnprintf formatting failed.\n");
        // Or maybe log the raw format string? Depends on desired error handling.
        // log_cstring("[FORMAT_ERROR]", 14);
        return;
    }

    // Check for truncation
    bool truncated = false;
    if (static_cast<size_t>(message_len) >= LOG_BUFFER_SIZE) {
        message_len = LOG_BUFFER_SIZE - 1; // Use the full buffer capacity
        truncated = true;
        // Optional: add truncation indicator? Requires buffer space.
        // strcpy(g_format_buffer + LOG_BUFFER_SIZE - 4, "...");
    }

    // Call the core logging function with the formatted C-string
    log_cstring(g_format_buffer, message_len);

     if (truncated) {
         // Maybe log a separate message about truncation?
         // Be careful to avoid infinite loops if logging itself truncates.
         // log_cstring("[Previous message truncated]", 27);
     }
}

// --- Core internal logging function ---
void Logger::log_cstring(const char* formatted_message, size_t message_len) const {
    if (!is_open_.load()) { // Quick check before heavier operations
         fprintf(stderr, "Logger Error: Log file is not open. Message: %.*s\n", (int)message_len, formatted_message);
        return;
    }

    // --- Prepare the full log entry *before* locking ---
    char timestamp_buf[64];
    size_t timestamp_len = getCurrentTimestamp(timestamp_buf, sizeof(timestamp_buf));

    // Using thread ID hash is reasonable. Direct thread ID formatting might be complex.
    auto thread_id_hash = std::hash<std::thread::id>{}(std::this_thread::get_id());

    // Use snprintf to assemble the final entry in the *second* thread-local buffer
    // Format: "YYYY-MM-DD HH:MM:SS.ms [Thread hash] message"
    int entry_len = snprintf(g_entry_buffer, sizeof(g_entry_buffer),
                             "%.*s [Thread %zu] %.*s\n", // Include newline here
                             (int)timestamp_len, timestamp_buf,
                             thread_id_hash,
                             (int)message_len, formatted_message);

    if (entry_len < 0) {
        fprintf(stderr, "Logger Error: snprintf failed during final entry assembly.\n");
        return;
    }

    // Check for final assembly truncation (less likely with larger buffer, but possible)
    if (static_cast<size_t>(entry_len) >= sizeof(g_entry_buffer)) {
        entry_len = sizeof(g_entry_buffer) - 1;
        // Maybe indicate this truncation too, e.g., by forcing last chars to "...\n"?
        // strcpy(g_entry_buffer + sizeof(g_entry_buffer) - 5, "...\n");
    }


    // --- Lock only for critical section (I/O) ---
    std::lock_guard<std::mutex> lock(mutex_);

    // Write to file (use raw write for potential performance)
    if (is_open_.load()) { // Re-check inside lock just in case state changed (though unlikely)
        log_file_.write(g_entry_buffer, entry_len);
        // log_file_.flush(); // AVOID flush unless absolutely necessary!
    }

    // Log to console if enabled
    if (log_to_console_.load()) {
        // Use C style fwrite to stdout for potentially better performance than iostreams
        fwrite(g_entry_buffer, 1, entry_len, stdout);
        // fflush(stdout); // AVOID flush unless needed
    }
}