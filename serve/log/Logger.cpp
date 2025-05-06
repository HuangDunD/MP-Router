#include "Logger.h"
#include <chrono>
#include <cstdio>
#include <algorithm>
#include <cstdarg>
#include <cstring>
#include <ctime>
#include <functional>
#include <iostream>
#include <mutex>
#include <thread>

// ---------- 常量 ----------
constexpr size_t LOG_BUFFER_SIZE = 2048; // 用户消息最大长度
constexpr size_t ENTRY_BUFFER_SIZE = LOG_BUFFER_SIZE + 128;

// ---------- 线程局部缓冲 ----------
namespace {
    thread_local char g_fmt_buf[LOG_BUFFER_SIZE];
    thread_local char g_entry_buf[ENTRY_BUFFER_SIZE];
}

// ---------- 辅助函数 ----------
static bool to_local_tm(const time_t &t, std::tm &out) {
#ifdef _WIN32
    return localtime_s(&out, &t) == 0;
#else
    return localtime_r(&t, &out) != nullptr;
#endif
}

static size_t build_timestamp(char *buf, size_t cap) {
    if (cap < 32) return 0;
    auto now = std::chrono::system_clock::now();
    auto t = std::chrono::system_clock::to_time_t(now);
    auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                  now.time_since_epoch()) %
              1000;

    std::tm tm{};
    if (!to_local_tm(t, tm)) return 0;

    size_t n = std::strftime(buf, cap, "%Y-%m-%d %H:%M:%S", &tm);
    if (n == 0 || n + 5 >= cap) return 0;

    int m = std::snprintf(buf + n, cap - n, ".%03lld",
                          static_cast<long long>(ms.count()));
    if (m < 0) return 0;
    buf[n + m] = '\0';
    return n + static_cast<size_t>(m);
}

// ---------- Logger 实现 ----------
Logger::Logger(const std::string &file, bool append)
    : log_file_(file,
                std::ios::binary | std::ios::out |
                (append ? std::ios::app : std::ios::trunc)) {
    if (!log_file_) {
        std::fprintf(stderr,
                     "Logger CRITICAL: failed to open %s\n", file.c_str());
        throw std::runtime_error("Logger open failed");
    }
    is_open_.store(true);
    log("Logger initialized. Output file: %s", file.c_str());
}

Logger::~Logger() {
    if (is_open_.load()) {
        log("Logger shutting down.");
        log_file_.flush();
    }
}

void Logger::set_log_to_console(bool on) {
    log_to_console_.store(on);
}

void Logger::log(const char *fmt, ...) const {
    if (!is_open_.load()) return;

    va_list ap;
    va_start(ap, fmt);
    int len = std::vsnprintf(g_fmt_buf, sizeof(g_fmt_buf), fmt, ap);
    va_end(ap);

    if (len < 0) return;
    size_t msg_len =
            static_cast<size_t>(std::min<int>(len, sizeof(g_fmt_buf) - 1));
    g_fmt_buf[msg_len] = '\0'; // 保证结束符
    log_cstring(g_fmt_buf, msg_len);
}

void Logger::log_cstring(const char *msg, size_t msg_len) const {
    if (!is_open_.load()) return;

    // 1) 组装完整条目（无锁）
    char ts[64];
    size_t ts_len = build_timestamp(ts, sizeof(ts));

    auto tid = std::hash<std::thread::id>{}(std::this_thread::get_id());

    int entry_len = std::snprintf(
        g_entry_buf, sizeof(g_entry_buf), "%.*s [T%zu] %.*s\n",
        static_cast<int>(ts_len), ts, tid, static_cast<int>(msg_len), msg);

    if (entry_len <= 0) return;
    size_t final_len =
            static_cast<size_t>(std::min<int>(entry_len, sizeof(g_entry_buf) - 1));
    g_entry_buf[final_len] = '\0';

    // 2) 进入 I/O 临界区
    {
        std::lock_guard<std::mutex> lk(mutex_);
        log_file_.write(g_entry_buf, static_cast<std::streamsize>(final_len));
    }
    if (log_to_console_.load()) {
        std::fwrite(g_entry_buf, 1, final_len, stdout);
    }
}
