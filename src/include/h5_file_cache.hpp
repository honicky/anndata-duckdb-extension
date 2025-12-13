#pragma once

#include <mutex>
#include <unordered_map>
#include <memory>
#include <string>
#include "h5_handles.hpp"
#include <hdf5.h>

namespace duckdb {

struct H5FileDeleter {
  void operator()(hid_t* id) const {
    if (id && *id >= 0) {
      // Force closing of all open objects under this file
      H5Fclose(*id);
    }
    delete id;
  }
};

class H5FileCache {
public:
  static std::shared_ptr<hid_t> Open(const std::string& path) {
    static H5FileCache& instance = Instance();
    std::lock_guard<std::mutex> guard(instance.mu_);
    
    // Check if we have a cached handle
    auto it = instance.map_.find(path);
    if (it != instance.map_.end()) {
      if (auto sp = it->second.lock()) {
        // Cached handle still alive
        return sp;
      }
      // Weak pointer expired, remove from map
      instance.map_.erase(it);
    }
    
    // Create a new shared handle with proper settings
    hid_t fapl = H5Pcreate(H5P_FILE_ACCESS);
    if (fapl < 0) {
      throw std::runtime_error("H5Pcreate failed");
    }
    
    // Strong close: ensures dependent objects don't dangle
    H5Pset_fclose_degree(fapl, H5F_CLOSE_STRONG);
    
    // Set library version bounds for consistency
    H5Pset_libver_bounds(fapl, H5F_LIBVER_V110, H5F_LIBVER_LATEST);
    
    // Open file in read-only mode
    // TODO: Consider adding H5F_ACC_SWMR_READ for better consistency
    hid_t file = H5Fopen(path.c_str(), H5F_ACC_RDONLY, fapl);
    H5Pclose(fapl);
    
    if (file < 0) {
      throw std::runtime_error("H5Fopen failed: " + path);
    }
    
    // Create shared pointer with custom deleter
    std::shared_ptr<hid_t> handle(new hid_t(file), H5FileDeleter{});
    
    // Store weak pointer in cache
    instance.map_[path] = handle;
    
    return handle;
  }
  
  // Clear the cache (useful for testing)
  static void Clear() {
    static H5FileCache& instance = Instance();
    std::lock_guard<std::mutex> guard(instance.mu_);
    instance.map_.clear();
  }
  
private:
  static H5FileCache& Instance() {
    static H5FileCache instance;
    return instance;
  }
  
  H5FileCache() = default;
  ~H5FileCache() = default;
  
  // Delete copy/move operations
  H5FileCache(const H5FileCache&) = delete;
  H5FileCache& operator=(const H5FileCache&) = delete;
  H5FileCache(H5FileCache&&) = delete;
  H5FileCache& operator=(H5FileCache&&) = delete;
  
  std::mutex mu_;
  std::unordered_map<std::string, std::weak_ptr<hid_t>> map_;
};

} // namespace duckdb