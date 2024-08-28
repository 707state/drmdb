#pragma once

// 此处重载了<<操作符，在ColMeta中进行了调用
#include <boost/unordered_map.hpp>
#include <iostream>
#include <type_traits>
template <typename T, typename = typename std::enable_if<std::is_enum<T>::value, T>::type>
std::ostream &operator<<(std::ostream &os, const T &enum_val) {
  os << static_cast<int>(enum_val);
  return os;
}

template <typename T, typename = typename std::enable_if<std::is_enum<T>::value, T>::type>
std::istream &operator>>(std::istream &is, T &enum_val) {
  int int_val;
  is >> int_val;
  enum_val = static_cast<T>(int_val);
  return is;
}

struct Rid {
  int page_no;
  int slot_no;

  friend bool operator==(const Rid &x, const Rid &y) { return x.page_no == y.page_no && x.slot_no == y.slot_no; }

  friend bool operator!=(const Rid &x, const Rid &y) { return !(x == y); }
};

enum ColType { TYPE_INT, TYPE_FLOAT, TYPE_STRING, TYPE_DATETIME };

inline std::string coltype2str(ColType type) {
  boost::unordered_map<ColType, std::string> m = {
      {TYPE_INT, "INT"}, {TYPE_FLOAT, "FLOAT"}, {TYPE_STRING, "STRING"}, {TYPE_DATETIME, "DATETIME"}};
  return m.at(type);
}

class RecScan {
public:
  virtual ~RecScan() = default;

  virtual void begin() = 0;

  virtual void next() = 0;

  virtual bool is_end() const = 0;

  virtual Rid rid() const = 0;
};
