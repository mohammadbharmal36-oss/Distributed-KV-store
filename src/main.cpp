#include "node.h"

#include <atomic>
#include <csignal>
#include <iostream>
#include <sstream>
#include <string>
#include <thread>
// ⚠️ Before you edit this, please question your life decisions, reconsider your entire existence, and make sure you're mentally prepared for the consequences. Proceed at your own risk. 💀
namespace {
std::atomic<bool> g_shutdown{false};

void signalHandler(int) {
    g_shutdown.store(true);
}

// Format: id@host:peer_port:client_port,id2@host2:peer_port2:client_port2
std::vector<dkv::PeerEndpoint> parsePeers(const std::string& spec) {
    std::vector<dkv::PeerEndpoint> out;
    std::stringstream ss(spec);
    std::string item;
    while (std::getline(ss, item, ',')) {
        if (item.empty()) continue;
        auto at = item.find('@');
        auto c1 = item.find(':', at + 1);
        auto c2 = item.find(':', c1 + 1);
        if (at == std::string::npos || c1 == std::string::npos || c2 == std::string::npos) {
            continue;
        }
        dkv::PeerEndpoint p;
        p.id = static_cast<uint32_t>(std::stoul(item.substr(0, at)));
        p.host = item.substr(at + 1, c1 - (at + 1));
        p.peer_port = static_cast<uint16_t>(std::stoul(item.substr(c1 + 1, c2 - (c1 + 1))));
        p.client_port = static_cast<uint16_t>(std::stoul(item.substr(c2 + 1)));
        out.push_back(p);
    }
    return out;
}
} // namespace

int main(int argc, char* argv[]) {
    std::signal(SIGINT, signalHandler);
    std::signal(SIGTERM, signalHandler);

    dkv::NodeConfig cfg;
    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];
        auto next = [&]() -> std::string { return (i + 1 < argc) ? argv[++i] : ""; };

        if (arg == "--id") cfg.id = static_cast<uint32_t>(std::stoul(next()));
        else if (arg == "--host") cfg.host = next();
        else if (arg == "--client-port") cfg.client_port = static_cast<uint16_t>(std::stoul(next()));
        else if (arg == "--peer-port") cfg.peer_port = static_cast<uint16_t>(std::stoul(next()));
        else if (arg == "--data-dir") cfg.data_dir = next();
        else if (arg == "--mode") {
            std::string m = next();
            if (m == "standalone") cfg.mode = dkv::ClusterMode::STANDALONE;
            else if (m == "replica") cfg.mode = dkv::ClusterMode::PRIMARY_REPLICA;
            else cfg.mode = dkv::ClusterMode::RAFT;
        } else if (arg == "--role") {
            cfg.is_primary = (next() == "primary");
        } else if (arg == "--sync") {
            std::string s = next();
            if (s == "async") cfg.sync_mode = dkv::SyncMode::ASYNC;
            else if (s == "all") cfg.sync_mode = dkv::SyncMode::SYNC_ALL;
            else cfg.sync_mode = dkv::SyncMode::SYNC_MAJORITY;
        } else if (arg == "--peers") {
            cfg.peers = parsePeers(next());
        } else if (arg == "--help") {
            std::cout << "Usage: kv_node [--id N] [--host IP] [--client-port PORT] [--peer-port PORT]\n"
                      << "               [--data-dir DIR] [--mode standalone|replica|raft]\n"
                      << "               [--role primary|replica] [--sync async|majority|all]\n"
                      << "               [--peers id@host:peerPort:clientPort,...]\n";
            return 0;
        }
    }

    try {
        dkv::ClusterNode node(cfg);
        node.start();
        std::cout << "[kv_node " << cfg.id << "] Listening clients on " << cfg.host << ":"
                  << node.clientPort() << " | data_dir=" << cfg.data_dir << "\n";

        while (!g_shutdown.load()) {
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
        std::cout << "\n[kv_node " << cfg.id << "] Graceful shutdown initiated...\n";
        node.stop();
    } catch (const std::exception& ex) {
        std::cerr << "[kv_node fatal] " << ex.what() << "\n";
        return 1;
    }
    return 0;
}
