#ifndef LOGGER_H
#define LOGGER_H

#include <string>
#include <fstream>
#include <mutex>
#include <memory>
#include <sstream>
#include <thread>
#include <utility> // Required for std::forward
#include <atomic>  // For atomic bool if needed, though mutex protects it here

class Logger {
public:
    explicit Logger(const std::string& log_filename, bool append = true);
    ~Logger();

    Logger(const Logger&) = delete;
    Logger& operator=(const Logger&) = delete;
    Logger(Logger&&) = default;
    Logger& operator=(Logger&&) = default;

    /**
     * @brief Logs a pre-formatted message string to the file (Core implementation).
     * Optionally logs to console based on log_to_console_ flag.
     * @param message The message string to log.
     */
    void log_string(const std::string& message);

    /**
     * @brief Logs a message constructed from various parts using variadic templates.
     * Example: logger.log("Processing ", 10, " items for user ", user_id);
     * @tparam Args Variadic template arguments.
     * @param args The parts of the message to log.
     */
    template <typename... Args>
    void log(Args&&... args) {
        std::stringstream ss;
        // C++17 fold expression to stream all arguments
        (ss << ... << std::forward<Args>(args));
        log_string(ss.str()); // Call the base log function
    }

    // --- MODIFICATION START: Added console logging control ---
    /**
     * @brief Enables or disables logging to standard output (std::cout).
     * @param enable True to enable console logging, false to disable (default).
     */
    void set_log_to_console(bool enable);
    // --- MODIFICATION END ---


private:
    std::ofstream log_file_;
    std::mutex mutex_;
    bool is_open_ = false;
    // --- MODIFICATION START: Added console logging flag ---
    bool log_to_console_ = false; // Default to false
    // --- MODIFICATION END ---


    std::string getCurrentTimestamp() const;
};


#endif // LOGGER_H