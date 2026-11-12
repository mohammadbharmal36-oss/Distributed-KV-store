#include "storage.h"

#include <mutex>

namespace dkv {

void StorageEngine::set(const std::string& key, std::string value) {
    std::unique_lock lock(mutex_);
    store_[key] = std::move(value);
}

std::optional<std::string> StorageEngine::get(const std::string& key) const {
    std::shared_lock lock(mutex_);
    auto it = store_.find(key);
    if (it == store_.end()) {
        return std::nullopt;
    }
    return it->second;
}

bool StorageEngine::del(const std::string& key) {
    std::unique_lock lock(mutex_);
    return store_.erase(key) > 0;
}

bool StorageEngine::exists(const std::string& key) const {
    std::shared_lock lock(mutex_);
    return store_.find(key) != store_.end();
}

bool StorageEngine::update(const std::string& key, std::string value) {
    std::unique_lock lock(mutex_);
    auto it = store_.find(key);
    if (it == store_.end()) {
        return false;
    }
    it->second = std::move(value);
    return true;
}

bool StorageEngine::apply(OpType op, const std::string& key, const std::string& value) {
    switch (op) {
        case OpType::SET:
            set(key, value);
            return true;
        case OpType::DELETE_OP:
            return del(key);
        case OpType::UPDATE:
            return update(key, value);
        case OpType::NOOP:
            return true;
    }
    return false;
}

size_t StorageEngine::size() const {
    std::shared_lock lock(mutex_);
    return store_.size();
}

void StorageEngine::clear() {
    std::unique_lock lock(mutex_);
    store_.clear();
    snapshot_index_ = 0;
    snapshot_term_ = 0;
}

SnapshotData StorageEngine::takeSnapshot(uint64_t last_index, uint64_t last_term) const {
    std::shared_lock lock(mutex_);
    SnapshotData snap;
    snap.last_included_index = last_index;
    snap.last_included_term = last_term;
    snap.kv = store_;
    return snap;
}

void StorageEngine::restoreSnapshot(SnapshotData snapshot) {
    std::unique_lock lock(mutex_);
    snapshot_index_ = snapshot.last_included_index;
    snapshot_term_ = snapshot.last_included_term;
    store_ = std::move(snapshot.kv);
}

uint64_t StorageEngine::lastSnapshotIndex() const {
    std::shared_lock lock(mutex_);
    return snapshot_index_;
}

uint64_t StorageEngine::lastSnapshotTerm() const {
    std::shared_lock lock(mutex_);
    return snapshot_term_;
}

} // namespace dkv
