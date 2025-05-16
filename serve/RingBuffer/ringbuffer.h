#include <vector>
#include <cstring>
#include <unistd.h>
#include <arpa/inet.h>
#include <atomic>
#include <mutex>
#include <iostream>
#include <cassert>

// 环形缓冲区，容量必须大于最大单条消息长度（SOCKET_BUFFER_SIZE*2即可）
class RingBuffer {
public:
    explicit RingBuffer(size_t cap)
        : buffer_(cap), cap_(cap), begin_(0), end_(0), size_(0) {}

    // 写入数据
    void write(const char* data, size_t n) {
        assert(n <= cap_ - size_); // 应确保不会溢出
        size_t tail = end_;
        for (size_t i = 0; i < n; ++i) {
            buffer_[tail] = data[i];
            tail = (tail + 1) % cap_;
        }
        end_ = tail;
        size_ += n;
    }

    // 从pos位置，读取n字节到dst
    void peek(size_t pos, char* dst, size_t n) const {
        size_t i = pos;
        for (size_t j = 0; j < n; ++j) {
            dst[j] = buffer_[(begin_ + i + j) % cap_];
        }
    }

    // 直接指针方式获得连续区间（仅当没有环绕时有效）
    const char* contiguous_data(size_t offset = 0) const {
        return &buffer_[(begin_ + offset) % cap_];
    }

    // 当前可用数据长度
    size_t size() const { return size_; }

    // 删除前n字节（消费掉）
    void pop(size_t n) {
        assert(n <= size_);
        begin_ = (begin_ + n) % cap_;
        size_ -= n;
    }

private:
    std::vector<char> buffer_;
    size_t cap_, begin_, end_, size_;
};
