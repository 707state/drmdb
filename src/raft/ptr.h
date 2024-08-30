#pragma once
#include <memory>
#include <utility>
namespace raft {
template <typename T> using ptr = std::shared_ptr<T>;
template <typename T> using wptr = std::weak_ptr<T>;
template <typename T, typename... Args> inline ptr<T> new_ptr(Args... args) {
  return std::make_shared<T>(std::forward<Args>(args)...);
}
} // namespace raft
