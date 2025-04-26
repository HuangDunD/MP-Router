#include "Logger.h"
#include <iostream> // Needed for std::cout, std::cerr, std::endl
#include <chrono>
#include <iomanip>
#include <sstream>
#include <stdexcept>
#include <thread> // Ensure thread is included for get_id

Logger::Logger(const std::string &log_filename, bool append) {
    std::ios_base::openmode mode = std::ios_base::out;
    if (append) {
        mode |= std::ios_base::app;
    } else {
        mode |= std::ios_base::trunc;
    }
    log_file_.open(log_filename, mode);
    if (!log_file_.is_open()) {
        std::cerr << "Logger CRITICAL ERROR: Could not open log file: " << log_filename << std::endl;
        throw std::runtime_error("Logger Error: Could not open log file: " + log_filename);
    }
    is_open_ = true;
    // Initial message will also respect the (default false) console log setting
    log_string("Logger initialized. Logging to file: " + log_filename);
}

Logger::~Logger() {
    if (log_file_.is_open()) {
        log_string("Logger shutting down.");
        log_file_.close();
    }
}

std::string Logger::getCurrentTimestamp() const {
    auto now = std::chrono::system_clock::now();
    auto now_c = std::chrono::system_clock::to_time_t(now);
    auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch()) % 1000;

    std::stringstream ss;
    // POSIX: Use localtime_r for thread-safety if available
    // std::tm now_tm;
    // localtime_r(&now_c, &now_tm);
    // ss << std::put_time(&now_tm, "%Y-%m-%d %H:%M:%S");
    // Windows: Use localtime_s for thread-safety if available
    // std::tm now_tm;
    // localtime_s(&now_tm, &now_c);
    // ss << std::put_time(&now_tm, "%Y-%m-%d %H:%M:%S");
    // Fallback (potentially not thread-safe):
    ss << std::put_time(std::localtime(&now_c), "%Y-%m-%d %H:%M:%S");

    ss << '.' << std::setw(3) << std::setfill('0') << ms.count();
    return ss.str();
}

// --- MODIFICATION START: Added setter implementation ---
void Logger::set_log_to_console(bool enable) {
    // Optional: Add lock if you want setting this to be thread-safe,
    // although it's often set once during initialization.
    // std::lock_guard<std::mutex> lock(mutex_);
    log_to_console_ = enable;
}

// --- MODIFICATION END ---

void Logger::log_string(const std::string &message) {
    // Construct the full log entry string *once*
    std::string timestamp = getCurrentTimestamp();
    auto thread_id_hash = std::hash<std::thread::id>{}(std::this_thread::get_id());
    std::stringstream entry_ss;
    entry_ss << timestamp << " [Thread " << thread_id_hash << "] " << message;
    std::string log_entry = entry_ss.str();

    // Lock only when accessing shared resources (file, console flag)
    std::lock_guard<std::mutex> lock(mutex_);

    // Log to file if open
    if (is_open_) {
        log_file_ << log_entry << std::endl;
        // log_file_.flush(); // Optional flush
    } else {
        // If file isn't open, maybe still log to console if enabled?
        // Or just log the file error to cerr. Current behavior logs error to cerr:
        std::cerr << "Logger Error: Log file is not open. Message: " << message << std::endl;
        // If you *also* want the message on cerr when the file isn't open:
        // std::cerr << log_entry << std::endl;
    }

    // --- MODIFICATION START: Conditional console output ---
    // Log to console if enabled
    if (log_to_console_) {
        std::cout << log_entry << std::endl;
    }
    // --- MODIFICATION END ---
}
