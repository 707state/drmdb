#pragma once
#include "common/context.h"
#include <cassert>
#include <cstddef>
#define RECORD_COUNT_LENGTH 40

class RecordPrinter {
  static constexpr size_t COL_WIDTH = 16;
  size_t num_cols;

public:
  explicit RecordPrinter(size_t num_cols_) : num_cols(num_cols_) { assert(num_cols_ > 0); }

  void print_separator(Context *context) const {
    for (size_t i = 0; i < num_cols; i++) {
      std::string str = "+" + std::string(COL_WIDTH + 2, '-');
      if (context->ellipsis_ == false && *context->offset_ + RECORD_COUNT_LENGTH + str.length() < BUFFER_LENGTH) {
        memcpy(context->data_send_ + *(context->offset_), str.c_str(), str.length());
        *(context->offset_) = *(context->offset_) + str.length();
      } else {
        context->ellipsis_ = true;
      }
    }
    std::string str = "+\n";
    if (context->ellipsis_ == false && *context->offset_ + RECORD_COUNT_LENGTH + str.length() < BUFFER_LENGTH) {
      memcpy(context->data_send_ + *(context->offset_), str.c_str(), str.length());
      *(context->offset_) = *(context->offset_) + str.length();
    } else {
      context->ellipsis_ = true;
    }
  }

  void print_record(const std::vector<std::string> &rec_str, Context *context) const {
    assert(rec_str.size() == num_cols);
    for (auto col : rec_str) {
      if (col.size() > COL_WIDTH) {
        col = col.substr(0, COL_WIDTH - 3) + "...";
      }
      // std::cout << "| " << std::setw(COL_WIDTH) << col << ' ';
      std::stringstream ss;
      ss << "| " << std::setw(COL_WIDTH) << col << " ";
      if (context->ellipsis_ == false && *context->offset_ + RECORD_COUNT_LENGTH + ss.str().length() < BUFFER_LENGTH) {
        memcpy(context->data_send_ + *(context->offset_), ss.str().c_str(), ss.str().length());
        *(context->offset_) = *(context->offset_) + ss.str().length();
      } else {
        context->ellipsis_ = true;
      }
    }
    std::string str = "|\n";
    if (context->ellipsis_ == false && *context->offset_ + RECORD_COUNT_LENGTH + str.length() < BUFFER_LENGTH) {
      memcpy(context->data_send_ + *(context->offset_), str.c_str(), str.length());
      *(context->offset_) = *(context->offset_) + str.length();
    }
  }

  static void print_record_count(size_t num_rec, Context *context) {
    // std::cout << "Total record(s): " << num_rec << '\n';
    std::string str = "";
    if (context->ellipsis_ == true) {
      str = "... ...\n";
    }
    str += "Total record(s): " + std::to_string(num_rec) + '\n';
    memcpy(context->data_send_ + *(context->offset_), str.c_str(), str.length());
    *(context->offset_) = *(context->offset_) + str.length();
  }
};
