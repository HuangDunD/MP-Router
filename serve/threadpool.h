#ifndef THREADPOOL_H
#define THREADPOOL_H

#include <vector>
#include <queue>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <functional>
#include <future>
#include <stdexcept>
#include <pqxx/pqxx> // PostgreSQL C++ library

#include "Logger.h" // Include the Logger header
#include "config.h"
// --- Thread Pool Class Definition (Paste the ThreadPool class code here) ---

thread_local std::vector<pqxx::connection*> connections_thread_local;
class ThreadPool {
public:
    ThreadPool(size_t threads, std::vector<std::string> &connections, Logger &lg);
    ThreadPool(size_t threads, Logger &lg);

    ~ThreadPool();

    template<class F, class... Args>
    auto enqueue(F &&f, Args &&... args)
        -> std::future<typename std::result_of<F(Args...)>::type>;

private:
    std::vector<std::thread> workers;
    std::queue<std::function<void()> > tasks;
    std::mutex queue_mutex;
    std::condition_variable condition;
    bool stop;
};

inline ThreadPool::ThreadPool(size_t threads, Logger &lg) : stop(false) {
    for (size_t i = 0; i < threads; ++i) {
        workers.emplace_back([this] {
            while (true) {
                std::function<void()> task; {
                    std::unique_lock<std::mutex> lock(this->queue_mutex);
                    this->condition.wait(lock, [this] { return this->stop || !this->tasks.empty(); });
                    if (this->stop && this->tasks.empty()) return;
                    task = std::move(this->tasks.front());
                    this->tasks.pop();
                }
                task();
            }
        });
    }
}

inline ThreadPool::ThreadPool(size_t threads, std::vector<std::string> &connections, Logger &lg) : stop(false) {
    for (size_t i = 0; i < threads; ++i) {
        workers.emplace_back([this, &connections, &lg] {
            // Initialize thread-local storage for connections
            for(size_t i = 0; i < connections.size(); ++i) {
                std::string conninfo = connections[i];
                try {
                    // Create a connection for each thread
                    pqxx::connection *conn = new pqxx::connection(conninfo);
                    if (!conn->is_open()) {
                        lg.error(" Failed to connect to the database. conninfo" + conninfo);
                    } else {
                        lg.info("Connected to the database successful.");
                    }
                    // Store the connection in thread-local storage
                    connections_thread_local.push_back(conn);
                } catch (const std::exception &e) {
                    lg.error("Error while connecting to KingBase: " + std::string(e.what()));
                }
            }
            while (true) {
                std::function<void()> task; {
                    std::unique_lock<std::mutex> lock(this->queue_mutex);
                    this->condition.wait(lock, [this] { return this->stop || !this->tasks.empty(); });
                    if (this->stop && this->tasks.empty()) return;
                    task = std::move(this->tasks.front());
                    this->tasks.pop();
                }
                task();
            }
        });
    }
}

template<class F, class... Args>
auto ThreadPool::enqueue(F &&f, Args &&... args)
    -> std::future<typename std::result_of<F(Args...)>::type> {
    using return_type = typename std::result_of<F(Args...)>::type;
    auto task = std::make_shared<std::packaged_task<return_type()> >(
        std::bind(std::forward<F>(f), std::forward<Args>(args)...)
    );
    std::future<return_type> res = task->get_future(); {
        std::unique_lock<std::mutex> lock(queue_mutex);
        if (stop) throw std::runtime_error("enqueue on stopped ThreadPool");
        tasks.emplace([task]() { (*task)(); });
    }
    condition.notify_one();
    return res;
}

inline ThreadPool::~ThreadPool() { {
        std::unique_lock<std::mutex> lock(queue_mutex);
        stop = true;
    }
    condition.notify_all();
    for (std::thread &worker: workers) worker.join();
}

// --- End Thread Pool Class Definition ---

#endif //THREADPOOL_H
