#include "client.h"
#include "server.h"

#include <atomic>
#include <cctype>
#include <chrono>
#include <iomanip>
#include <iostream>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

namespace {

void printResponse(const dkv::ServerResponse& r) {
    using dkv::StatusCode;
    switch (r.status) {
        case StatusCode::OK:
            std::cout << (r.value.empty() ? r.message : r.value) << "\n";
            break;
        case StatusCode::NOT_FOUND:
            std::cout << "(nil)\n";
            break;
        case StatusCode::EXISTS_TRUE:
            std::cout << "(integer) 1\n";
            break;
        case StatusCode::EXISTS_FALSE:
            std::cout << "(integer) 0\n";
            break;
        case StatusCode::NOT_LEADER:
            std::cout << "(redirect) Not leader. Hint: " << r.leader_hint << "\n";
            break;
        default:
            std::cout << "(error) " << r.message << "\n";
            break;
    }
}

void runBenchmark(const std::string& host, uint16_t port, size_t threads, size_t total_ops) {
    dkv::LatencyTracker set_lat(total_ops);
    dkv::LatencyTracker get_lat(total_ops);
    dkv::LatencyTracker del_lat(total_ops);

    const size_t ops_per_thread = total_ops / threads;
    const auto wall_t0 = std::chrono::steady_clock::now();

    std::vector<std::thread> workers;
    workers.reserve(threads);
    for (size_t tid = 0; tid < threads; ++tid) {
        workers.emplace_back([&, tid] {
            dkv::KvClient cli(host, port, 5000, true);
            cli.connect();
            for (size_t i = 0; i < ops_per_thread; ++i) {
                std::string k = "bench:" + std::to_string(tid) + ":" + std::to_string(i);
                auto t0 = std::chrono::steady_clock::now();
                cli.set(k, "payload_value_128b_benchmark_record");
                auto t1 = std::chrono::steady_clock::now();
                set_lat.record(std::chrono::duration_cast<std::chrono::microseconds>(t1 - t0).count());

                t0 = std::chrono::steady_clock::now();
                cli.get(k);
                t1 = std::chrono::steady_clock::now();
                get_lat.record(std::chrono::duration_cast<std::chrono::microseconds>(t1 - t0).count());

                if (i % 5 == 0) {
                    t0 = std::chrono::steady_clock::now();
                    cli.del(k);
                    t1 = std::chrono::steady_clock::now();
                    del_lat.record(std::chrono::duration_cast<std::chrono::microseconds>(t1 - t0).count());
                }
            }
        });
    }
    for (auto& w : workers) w.join();

    const auto wall_t1 = std::chrono::steady_clock::now();
    double secs = std::chrono::duration<double>(wall_t1 - wall_t0).count();

    auto printRow = [&](const char* label, const dkv::LatencySnapshot& s) {
        double rps = static_cast<double>(s.total_requests) / secs;
        std::cout << std::left << std::setw(10) << label
                  << " | Ops: " << std::setw(8) << s.total_requests
                  << " | Throughput: " << std::setw(10) << std::fixed << std::setprecision(1) << rps << " req/s"
                  << " | Avg: " << std::setw(6) << s.avg_us << " us"
                  << " | P95: " << std::setw(6) << s.p95_us << " us"
                  << " | P99: " << std::setw(6) << s.p99_us << " us\n";
    };

    std::cout << "\n=== Distributed KV Store Benchmark (" << threads << " concurrent clients) ===\n";
    printRow("SET", set_lat.snapshot());
    printRow("GET", get_lat.snapshot());
    printRow("DELETE", del_lat.snapshot());
}

} // namespace

int main(int argc, char* argv[]) {
    std::string host = "127.0.0.1";
    uint16_t port = 7001;
    bool benchmark = false;
    size_t threads = 8;
    size_t requests = 20000;

    for (int i = 1; i < argc; ++i) {
        std::string a = argv[i];
        auto next = [&]() -> std::string { return (i + 1 < argc) ? argv[++i] : ""; };
        if (a == "--host") host = next();
        else if (a == "--port") port = static_cast<uint16_t>(std::stoul(next()));
        else if (a == "--benchmark") benchmark = true;
        else if (a == "--threads") threads = std::stoul(next());
        else if (a == "--requests") requests = std::stoul(next());
    }

    if (benchmark) {
        runBenchmark(host, port, threads, requests);
        return 0;
    }

    dkv::KvClient client(host, port);
    try {
        client.connect();
        std::cout << "Connected to " << host << ":" << port
                  << ". Commands: SET k v | GET k | UPDATE k v | DELETE k | EXISTS k | SNAPSHOT | STATS | QUIT\n";
    } catch (const std::exception& e) {
        std::cerr << "Initial connect warning: " << e.what() << " (will retry on command)\n";
    }

    std::string line;
    while (std::cout << "kv> " && std::getline(std::cin, line)) {
        std::istringstream iss(line);
        std::string cmd, key, val;
        if (!(iss >> cmd)) continue;
        for (auto& c : cmd) c = static_cast<char>(std::toupper(static_cast<unsigned char>(c)));

        if (cmd == "QUIT" || cmd == "EXIT") break;
        try {
            if (cmd == "SET") {
                iss >> key;
                std::getline(iss >> std::ws, val);
                printResponse(client.set(key, val));
            } else if (cmd == "GET") {
                iss >> key;
                printResponse(client.get(key));
            } else if (cmd == "UPDATE") {
                iss >> key;
                std::getline(iss >> std::ws, val);
                printResponse(client.update(key, val));
            } else if (cmd == "DELETE" || cmd == "DEL") {
                iss >> key;
                printResponse(client.del(key));
            } else if (cmd == "EXISTS") {
                iss >> key;
                printResponse(client.exists(key));
            } else if (cmd == "SNAPSHOT") {
                printResponse(client.triggerSnapshot());
            } else if (cmd == "STATS") {
                printResponse(client.stats());
            } else if (cmd == "PING") {
                printResponse(client.ping());
            } else {
                std::cout << "Unknown command: " << cmd << "\n";
            }
        } catch (const std::exception& ex) {
            std::cerr << "Connection error: " << ex.what() << "\n";
            client.disconnect();
        }
    }
    return 0;
}
