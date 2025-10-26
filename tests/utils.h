#pragma once

#include <assert.h>
#include <bits/stdc++.h>
#include <stdlib.h>
#include <sys/time.h>
#include <boost/algorithm/string.hpp>
#include <fstream>
#include <sstream>
#include <vector>
#include "../include/buffer_pool_manager.h"

using namespace std;

class ZipfGenerator {
 public:
  // N: number of distinct keys (ranks 1..N)
  // s: exponent (zipf parameter, >0). commonly 0.6 - 1.2
  // seed: optional
  ZipfGenerator(size_t N, double s, bool gen_file, size_t thread_id = 0)
      : N_(N), alpha_(s), gen_(std::random_device{}()) {
    if (N_ == 0)
      throw std::invalid_argument("N must be > 0");
    if (alpha_ <= 0.0)
      throw std::invalid_argument("s must be > 0");

    trace_dir_ = getenv("TRACE_DIR");
    if (trace_dir_.empty()) {
      assert(false);
    }

    if (!gen_file) {
      file_.open(GetFilePath(thread_id), std ::ios::in | std::ios::out);
      gbp::GBPLOG << GetFilePath(thread_id);
      assert(file_.is_open());
      return;
    }

    weights_.resize(N_);
    for (size_t i = 0; i < N_; i++) {
      weights_[i] =
          1.0 / std::pow(i + 1, alpha_);  // 幂律分布公式: P(x) ∝ x^(-alpha)
    }
    std::shuffle(weights_.begin(), weights_.end(), gen_);
    global_power_law_dist_ =
        std::discrete_distribution<size_t>(weights_.begin(), weights_.end());
  }

  void GenToFile(size_t thread_num, size_t query_num_perthread) {
    do {
      std::ostringstream oss;
      oss << std::fixed << std::setprecision(2) << alpha_;
      auto file_path = GetFilePath(thread_num - 1);
      gbp::GBPLOG << "gen zipf " << query_num_perthread << " to file "
                  << file_path;
      file_.open(file_path, std ::ios::in | std::ios::out | std::ios::trunc);
      assert(file_.is_open());

      for (size_t i = 0; i < query_num_perthread; ++i) {
        file_ << next() << endl;
        if (i % 1000000 == 0)
          gbp::GBPLOG << "gen zipf " << i << " / " << query_num_perthread;
      }
      file_.close();
    } while (--thread_num > 0);
  }
  size_t GetFromFile() {
    std::string line;
    if (std::getline(file_, line)) {
      return std::stoull(line);
    }
    return std::numeric_limits<size_t>::max();
  }

  // 返回 0-based index（对应 rank = index+1）
  size_t next() { return global_power_law_dist_(gen_); }
  std::string GetFilePath(size_t thread_id) {
    std::ostringstream oss;
    oss << std::fixed << std::setprecision(2) << alpha_;
    auto file_path = trace_dir_ + "zipf/" + oss.str() + "/" +
                     std::to_string(N_) + "/" + std::to_string(thread_id) +
                     ".txt";
    std::filesystem::path file_path_tmp(file_path);
    std::filesystem::create_directories(file_path_tmp.parent_path());
    return file_path;
  }

 private:
  size_t N_;
  double alpha_;
  std::vector<long double> weights_;
  std::discrete_distribution<size_t> global_power_law_dist_;
  std::mt19937 gen_;

  std::fstream file_;
  std::string trace_dir_;
};
