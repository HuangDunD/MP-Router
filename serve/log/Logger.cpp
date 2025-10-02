// log_sys.cpp (or continue in the same file)
 #include "Logger.h"

// --- Logger Implementation ---

Logger::Logger()
    : target(LogTarget::TERMINAL_ONLY),
      min_level(LogLevel::DEBUG),
      buffer_capacity_bytes(DEFAULT_BUFFER_CAPACITY) {
        std::lock_guard<std::mutex> lk(mutex_);
        char time_buffer[64];
        formatCurrentTime(time_buffer, sizeof(time_buffer));
        std::cout << "[WELCOME] " << __FILE__ << " " << time_buffer << " : === Start logging ===\n";
        std::cout.flush(); // Flush welcome message for terminal
}

Logger::Logger(LogTarget target, LogLevel min_level, const std::string& file_path, size_t buffer_cap_bytes)
    : target(target),
      min_level(min_level),
      path(file_path),
      buffer_capacity_bytes(buffer_cap_bytes) {

    char time_buffer[64];
    formatCurrentTime(time_buffer, sizeof(time_buffer));
    const char* welcome_prefix = "[WELCOME] ";
    const char* welcome_suffix = " : === Start logging ===\n";

    if (this->target == LogTarget::FILE_ONLY || this->target == LogTarget::FILE_AND_TERMINAL) {
        if (this->path.empty()) {
            std::cerr << welcome_prefix << __FILE__ << " " << time_buffer
                      << " : ERROR: Log target includes file, but no path provided. Defaulting to TERMINAL_ONLY.\n";
            std::cerr.flush();
            this->target = LogTarget::TERMINAL_ONLY; // Fallback
        } else {
            std::lock_guard<std::mutex> lk(mutex_);
            // 如果日志文件已存在则先删除
            std::ifstream test_exist(this->path);
            if (test_exist.good()) {
                test_exist.close();
                std::remove(this->path.c_str());
            }
            outfile.open(this->path, std::ios::out | std::ios::app);
            if (!outfile.is_open()) {
                std::cerr << welcome_prefix << __FILE__ << " " << time_buffer
                          << " : ERROR: Failed to open log file: " << this->path
                          << ". Defaulting to TERMINAL_ONLY if was FILE_ONLY.\n";
                std::cerr.flush();
                if (this->target == LogTarget::FILE_ONLY) this->target = LogTarget::TERMINAL_ONLY;
                // If FILE_AND_TERMINAL, it effectively becomes TERMINAL_ONLY regarding file operations
            } else {
                 // Write welcome message directly to file, bypassing buffer, and flush it.
                 outfile << welcome_prefix << __FILE__ << " " << time_buffer << welcome_suffix;
                 outfile.flush();
            }
        }
    }

    if (this->target == LogTarget::TERMINAL_ONLY || this->target == LogTarget::FILE_AND_TERMINAL) {
        std::cout << welcome_prefix << __FILE__ << " " << time_buffer << welcome_suffix;
        std::cout.flush(); // Flush welcome message for terminal
    }
}

Logger::~Logger() {
    std::lock_guard<std::mutex> lk(mutex_);
    shutting_down_.store(true, std::memory_order_relaxed);
    bool terminal_goodbye_needed = (target == LogTarget::TERMINAL_ONLY || target == LogTarget::FILE_AND_TERMINAL);

    if (outfile.is_open()) {
        char time_buffer[64];
        formatCurrentTime(time_buffer, sizeof(time_buffer));
        // Add goodbye message to the buffer before the final flush
        file_log_buffer << "[GOODBYE] " << __FILE__ << " " << time_buffer << " : === End logging (file) ===\n";
        flush_file_buffer(); // Flush any remaining logs including the goodbye message
        outfile.close();
    }

    if (terminal_goodbye_needed) {
        // Avoid duplicate goodbye if file operations failed and it fell back to terminal only AND already logged goodbye
        // This check is a bit complex, simpler is just to print if terminal was an intended target
        char time_buffer[64];
        formatCurrentTime(time_buffer, sizeof(time_buffer));
        std::cout << "[GOODBYE] " << __FILE__ << " " << time_buffer << " : === End logging (terminal) ===\n";
        std::cout.flush();
    }
}

void Logger::setMinLevel(LogLevel new_level) {
    this->min_level = new_level;
}

void Logger::output(const std::string& text, LogLevel current_message_level) {
    std::lock_guard<std::mutex> lk(mutex_);
    if (shutting_down_.load(std::memory_order_relaxed)) {
        return;
    }
    if (current_message_level < this->min_level) {
        return;
    }

    char time_buffer[80];
    formatCurrentTime(time_buffer, sizeof(time_buffer));
    const char* level_str = levelToString(current_message_level);

    // Terminal output (immediate)
    if (target == LogTarget::TERMINAL_ONLY || target == LogTarget::FILE_AND_TERMINAL) {
        std::cout << level_str << __FILE__ << " " << time_buffer << " : " << text << '\n';
        if (current_message_level == LogLevel::ERROR) {
            std::cout.flush();
        }
    }

    // File output (buffered)
    if (target == LogTarget::FILE_ONLY || target == LogTarget::FILE_AND_TERMINAL) {
        if (outfile.is_open()) {
            // Write to the internal stringstream buffer first
            file_log_buffer <<level_str<< " " << time_buffer << " : " << text << '\n';

            // Check conditions for flushing
            if (current_message_level == LogLevel::ERROR) {
                flush_file_buffer(); // Immediate flush for errors
            } else {
                // Prefer checking buffer string size to avoid tellp() returning -1 when state is bad
                const std::string& buf_ref = file_log_buffer.str();
                if (buf_ref.size() >= buffer_capacity_bytes) {
                    flush_file_buffer();
                }
            }
        }
    }
}

void Logger::flush() {
    // Flush file buffer if applicable
    if (target == LogTarget::FILE_ONLY || target == LogTarget::FILE_AND_TERMINAL) {
        if (outfile.is_open()) { // Redundant check if flush_file_buffer also checks, but safe
            flush_file_buffer();
        }
    }
    // Flush terminal output if applicable
    if (target == LogTarget::TERMINAL_ONLY || target == LogTarget::FILE_AND_TERMINAL) {
        std::cout.flush();
    }
}

void Logger::debug(const std::string& text) {
    output(text, LogLevel::DEBUG);
}

void Logger::info(const std::string& text) {
    output(text, LogLevel::INFO);
}

void Logger::warning(const std::string& text) {
    output(text, LogLevel::WARNING);
}

void Logger::error(const std::string& text) {
    output(text, LogLevel::ERROR);
}

/*
// --- Example Usage (main.cpp) ---
// #include "log_sys.h"
// #include <string>
// #include <thread> // For testing flush with time
// #include <chrono> // For std::chrono::seconds

int main() {
    std::cout << "--- Default Logger (Terminal, DEBUG) ---\n";
    Logger defaultLogger;
    defaultLogger.info("Info from default logger.");
    defaultLogger.debug("Debug from default logger.");

    std::cout << "\n--- File and Terminal Logger (INFO level, 128 bytes buffer) ---\n";
    Logger fileAndTerminalLogger(Logger::LogTarget::FILE_AND_TERMINAL, Logger::LogLevel::INFO, "app.log", 128);
    fileAndTerminalLogger.debug("This DEBUG message won't be shown or logged (below INFO).");
    fileAndTerminalLogger.info("Short info 1."); // ~50-60 bytes
    fileAndTerminalLogger.info("Short info 2."); // ~50-60 bytes, buffer ~100-120. Not flushed yet.
    fileAndTerminalLogger.warning("A warning message that is a bit longer to ensure buffer fills."); // This should trigger flush based on size.

    std::cout << "Pausing for a moment to check app.log before error..." << std::endl;
    // std::this_thread::sleep_for(std::chrono::seconds(5)); // If you want to check file manually

    fileAndTerminalLogger.error("This ERROR message will flush immediately.");
    fileAndTerminalLogger.info("Another info after error."); // Will be in buffer

    std::cout << "\n--- File Only Logger (WARNING level) ---\n";
    Logger fileOnlyLogger(Logger::LogTarget::FILE_ONLY, Logger::LogLevel::WARNING, "errors.log");
    fileOnlyLogger.info("This INFO won't be logged (below WARNING).");
    fileOnlyLogger.warning("This WARNING will be only in errors.log (buffered).");
    fileOnlyLogger.error("This ERROR will be only in errors.log (immediate flush).");
    fileOnlyLogger.warning("Another warning (buffered).");
    // Remaining messages in fileOnlyLogger flushed on destruction.

    std::cout << "\n--- Manual Flush Demo ---" << std::endl;
    Logger manualFlushLogger(Logger::LogTarget::FILE_ONLY, Logger::LogLevel::DEBUG, "manual_flush.log", 2048);
    manualFlushLogger.info("Message 1 for manual flush log.");
    manualFlushLogger.info("Message 2 for manual flush log.");
    std::cout << "Content of manual_flush.log might not be visible yet." << std::endl;
    // std::this_thread::sleep_for(std::chrono::seconds(5));
    manualFlushLogger.flush();
    std::cout << "Manual flush called. Check manual_flush.log." << std::endl;
    manualFlushLogger.info("Message 3 after manual flush.");

    std::cout << "\n--- End of main, destructors will be called, flushing remaining buffers. ---\n";
    return 0;
}
*/