#pragma once

#include <assert.h>
#include <fstream>
#include <iostream>
#include <memory>
#include <sstream>
#include <string>
#include <vector>

namespace csv {

class CSVLoader {
 public:
  CSVLoader() = default;
  CSVLoader(const std::string& file_path, char delimiter = '|') {
    assert(load(file_path, delimiter));
  }

  // 加载CSV文件
  bool load(const std::string& file_path, char delimiter = '|') {
    std::ifstream file(file_path);
    if (!file.is_open()) {
      return false;
    }

    // 读取表头
    std::string header_line;
    if (!std::getline(file, header_line)) {
      return false;
    }
    headers_ = split_line(header_line, delimiter);

    // 读取数据
    std::string line;
    while (std::getline(file, line)) {
      data_.push_back(split_line(line, delimiter));
    }

    file.close();
    return true;
  }

  // 获取列名
  const std::vector<std::string>& get_headers() const { return headers_; }

  // 获取第i行所有元素
  const std::vector<std::string> get_row(size_t i) const {
    if (i >= data_.size()) {
      return std::vector<std::string>();
    }
    return data_[i];
  }

  // 获取第i列所有元素
  const std::vector<std::string> get_column(size_t i) const {
    if (i >= headers_.size()) {
      return std::vector<std::string>();
    }
    std::vector<std::string> column;
    for (const auto& row : data_) {
      if (i < row.size()) {
        column.push_back(row[i]);
      }
    }
    return column;
  }

  // 获取第i行j列的元素
  const std::string get_element(size_t i, size_t j) const {
    if (i >= data_.size() || j >= headers_.size()) {
      return "";
    }
    if (j >= data_[i].size()) {
      return "";
    }
    return data_[i][j];
  }

  // 获取行数
  size_t row_count() const { return data_.size(); }

  // 获取列数
  size_t column_count() const { return headers_.size(); }

 private:
  std::vector<std::string> split_line(const std::string& line, char delimiter) {
    std::vector<std::string> tokens;
    std::stringstream ss(line);
    std::string token;
    while (std::getline(ss, token, delimiter)) {
      tokens.push_back(token);
    }
    return tokens;
  }

  std::vector<std::string> headers_;
  std::vector<std::vector<std::string>> data_;
};

}  // namespace csv
