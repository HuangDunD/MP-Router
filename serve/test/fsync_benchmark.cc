// fsync_benchmark.cc
// 写入两个文件，比较在并发/顺序下使用 fsync 和 fdatasync 的性能差异

#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <errno.h>

#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>
#include <thread>
#include <vector>
#include <chrono>
#include <iostream>

using namespace std;

static void fail(const char* msg) {
    perror(msg);
    exit(1);
}

int do_open(const char* path) {
    int fd = open(path, O_CREAT | O_RDWR, 0644);
    if (fd < 0) fail("open");
    // Ensure file is empty immediately after opening to avoid leftover data
    if (ftruncate(fd, 0) < 0) fail("ftruncate_on_open");
    return fd;
}

using sync_fn_t = int(*)(int);

int main(int argc, char** argv) {
    // 参数（可按需修改或通过命令行扩展）
    const char* f1 = "./fsync_bench_file1.bin";
    const char* f2 = "./fsync_bench_file2.bin";
    const size_t block_size = 4096; // 每次写入大小
    const size_t iterations = 1000; // 每个迭代写一次并 sync

    // 预分配缓冲
    vector<char> buf(block_size, 'A');

    int fd1 = do_open(f1);
    int fd2 = do_open(f2);

    auto run_once = [&](const char* sync_name, sync_fn_t syncf, bool concurrent, bool in_place) {
        // 清空文件
        if (ftruncate(fd1, 0) < 0) fail("ftruncate1");
        if (ftruncate(fd2, 0) < 0) fail("ftruncate2");

        // For in-place writes, pre-allocate space
        if (in_place) {
            if (write(fd1, buf.data(), block_size) != (ssize_t)block_size) fail("init write1");
            if (write(fd2, buf.data(), block_size) != (ssize_t)block_size) fail("init write2");
        }

        auto phase_begin = std::chrono::steady_clock::now();
        std::chrono::nanoseconds total_sync_ns(0);

        if (concurrent) {
            std::chrono::nanoseconds sync_ns1(0);
            std::chrono::nanoseconds sync_ns2(0);

            thread t1([&] {
                for (size_t i = 0; i < iterations; ++i) {
                    if (in_place) lseek(fd1, 0, SEEK_SET);
                    ssize_t w1 = write(fd1, buf.data(), block_size);
                    if (w1 != (ssize_t)block_size) fail("write1");
                    auto sync_begin = std::chrono::steady_clock::now();
                    if (syncf(fd1) != 0) fail("sync1");
                    auto sync_end = std::chrono::steady_clock::now();
                    sync_ns1 += std::chrono::duration_cast<std::chrono::nanoseconds>(sync_end - sync_begin);
                }
            });

            thread t2([&] {
                for (size_t i = 0; i < iterations; ++i) {
                    if (in_place) lseek(fd2, 0, SEEK_SET);
                    ssize_t w2 = write(fd2, buf.data(), block_size);
                    if (w2 != (ssize_t)block_size) fail("write2");
                    auto sync_begin = std::chrono::steady_clock::now();
                    if (syncf(fd2) != 0) fail("sync2");
                    auto sync_end = std::chrono::steady_clock::now();
                    sync_ns2 += std::chrono::duration_cast<std::chrono::nanoseconds>(sync_end - sync_begin);
                }
            });

            t1.join();
            t2.join();
            total_sync_ns = sync_ns1 + sync_ns2;
        } else {
            // 串行模式只对单个文件(fd1)进行写+sync，用于计算单次写入的平均延迟
            for (size_t i = 0; i < iterations; ++i) {
                if (in_place) lseek(fd1, 0, SEEK_SET);
                // 写入单个文件（不计入 sync 时间）
                ssize_t w1 = write(fd1, buf.data(), block_size);
                if (w1 != (ssize_t)block_size) fail("write1");

                // 测量 sync 时间（仅 fd1）
                auto t0 = std::chrono::steady_clock::now();
                if (syncf(fd1) != 0) fail("sync1");
                auto t1time = std::chrono::steady_clock::now();
                total_sync_ns += std::chrono::duration_cast<std::chrono::nanoseconds>(t1time - t0);
            }
        }

        auto phase_end = std::chrono::steady_clock::now();
        auto total_phase_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(phase_end - phase_begin);

        double total_phase_ms = total_phase_ns.count() / 1e6;
        double total_ms = total_sync_ns.count() / 1e6;
        size_t total_ops = concurrent ? (iterations * 2) : iterations;
        double avg_phase_ms_per_iter = total_phase_ms / double(total_ops);
        double avg_sync_ms_per_iter = total_ms / double(total_ops);
        const char* write_mode = in_place ? "in-place" : "append";
        printf("%s | %s | %s | iterations=%zu block=%zu -> total_phase=%.3f ms avg_phase_per_iter=%.6f ms sync_only_total=%.3f ms avg_sync_per_iter=%.6f ms\n",
               sync_name,
               concurrent ? "concurrent" : "sequential",
               write_mode,
               iterations, block_size,
               total_phase_ms, avg_phase_ms_per_iter,
               total_ms, avg_sync_ms_per_iter);
    };

    printf("Starting fsync/fdatasync benchmark:\n  files: %s , %s\n  iterations: %zu  block_size: %zu\n\n",
           f1, f2, iterations, block_size);

    // 先 warm-up 一次
    if (write(fd1, buf.data(), block_size) != (ssize_t)block_size) fail("warm write1");
    if (write(fd2, buf.data(), block_size) != (ssize_t)block_size) fail("warm write2");
    fsync(fd1);
    fsync(fd2);

    // 运行八种组合：两种写模式 × 两种同步方式 × 两种并发模式
    printf("\n=== Append mode ===\n");
    run_once("fsync", [](int fd){ return fsync(fd); }, false, false);
    run_once("fsync", [](int fd){ return fsync(fd); }, true, false);
    run_once("fdatasync", [](int fd){ return fdatasync(fd); }, false, false);
    run_once("fdatasync", [](int fd){ return fdatasync(fd); }, true, false);

    printf("\n=== In-place mode ===\n");
    run_once("fsync", [](int fd){ return fsync(fd); }, false, true);
    run_once("fsync", [](int fd){ return fsync(fd); }, true, true);
    run_once("fdatasync", [](int fd){ return fdatasync(fd); }, false, true);
    run_once("fdatasync", [](int fd){ return fdatasync(fd); }, true, true);

    // 关闭
    close(fd1);
    close(fd2);

    printf("Benchmark finished.\n");
    return 0;
}
