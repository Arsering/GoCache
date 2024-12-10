#include <vector>

#include "column.h"
#include "column_family.h"
#include "mutable_csr.h"

namespace test {

class Vertex {
 public:
  enum class ColumnType {
    kFixedLength,
    kDynamicString,
    kDynamicEdgeList,
  };
  enum class EdgeType {
    kint32,
    kdate,
    knone,
  };
  struct ColumnConfiguration {
   public:
    ColumnConfiguration(size_t property_id, size_t column_family_id,
                        ColumnType column_type, EdgeType edge_type,
                        size_t column_length)
        : property_id(property_id),
          column_family_id(column_family_id),
          column_type(column_type),
          edge_type(edge_type),
          column_length(column_length) {}
    size_t property_id;
    size_t column_family_id;
    ColumnType column_type;
    EdgeType edge_type;
    size_t column_length;
  };

  class DataPerColumnFamily {
   public:
    DataPerColumnFamily() {
      fixed_length_column_family = new FixedLengthColumnFamily();
      dynamic_length_column_family = new mmap_array<char>();
      dynamic_length_column_family_size = 0;
      gbp::GBPLOG << "cp";
    }
    ~DataPerColumnFamily() {
      gbp::GBPLOG << "cp";

      if (fixed_length_column_family != nullptr) {
        delete fixed_length_column_family;
        fixed_length_column_family = nullptr;
      }

      // if (dynamic_length_column_family != nullptr) {
      //   delete dynamic_length_column_family;
      //   dynamic_length_column_family = NULL;
      // }
      // for (auto& csr : csr) {
      //   delete csr;
      // }
    }
    FixedLengthColumnFamily* fixed_length_column_family = nullptr;
    mmap_array<char>* dynamic_length_column_family = nullptr;
    size_t dynamic_length_column_family_size = 0;
    std::vector<mmap_array_base*>
        csr;  // csr之所以是一个vector，是因为一个vertex可能有多个edge_list，不同的edge的size可能不同，放在同一个mmap_array中不便于对齐
  };

  struct ColumnToColumnFamily {
    ColumnToColumnFamily(size_t property_id, ColumnType column_type,
                         size_t column_family_id, size_t edge_list_id,
                         size_t column_id_in_column_family)
        : property_id(property_id),
          column_type(column_type),
          column_family_id(column_family_id),
          edge_list_id(edge_list_id),
          column_id_in_column_family(column_id_in_column_family) {}
    ~ColumnToColumnFamily() = default;

    size_t property_id;
    ColumnType column_type;
    size_t column_family_id;
    size_t edge_list_id;
    size_t column_id_in_column_family;
  };

  Vertex() = default;
  ~Vertex() = default;
  // tuple<property_id, column_family_id, column_type, edge_type>
  void init(const std::vector<std::vector<ColumnConfiguration>>&
                column_configurations) {
    auto column_family_id = 0;
    auto column_id = 0;
    auto edge_list_column_id = 0;

    for (auto& column_configurations_tmp : column_configurations) {
      // 初始化每一个Column Family
      std::vector<size_t> column_lengths;  // 每个column的长度
      auto string_mark = false;            // 是否有string类型的column
      datas_of_all_column_family_.emplace_back();

      // 初始化每一个Column
      auto column_id_in_column_family = 0;  // 在column family中的id
      // for (auto& column_configuration : column_configurations_tmp) {
      //   column_lengths.push_back(column_configuration.column_length);
      //   column_to_column_familys_.emplace_back(
      //       column_configuration.property_id,
      //       column_configuration.column_type, column_family_id,
      //       std::numeric_limits<size_t>::max(),
      //       column_id_in_column_family++);
      //   switch (column_configuration.column_type) {
      //   case ColumnType::kFixedLength: {
      //     break;
      //   }
      //   case ColumnType::kDynamicString: {
      //     string_mark = true;
      //     break;
      //   }
      //   case ColumnType::kDynamicEdgeList: {
      //     switch (column_configuration.edge_type) {
      //     case EdgeType::kint32: {
      //       auto mmap_array_ptr = new mmap_array<int32_t>();
      //       mmap_array_ptr->open(
      //           "column_family_" + std::to_string(column_family_id) + "_" +
      //               std::to_string(edge_list_column_id) + ".csr",
      //           false);
      //       //
      //       datas_of_all_column_family_.back().csr.emplace_back(mmap_array_ptr);
      //       break;
      //     }
      //     case EdgeType::kdate: {
      //       auto mmap_array_ptr = new mmap_array<uint64_t>();
      //       mmap_array_ptr->open(
      //           "column_family_" + std::to_string(column_family_id) + "_" +
      //               std::to_string(edge_list_column_id) + ".csr",
      //           false);
      //       //
      //       datas_of_all_column_family_.back().csr.emplace_back(mmap_array_ptr);
      //       break;
      //     }
      //     case EdgeType::knone: {
      //       break;
      //     }
      //     }
      //     column_to_column_familys_.back().edge_list_id =
      //     edge_list_column_id++; break;
      //   }
      //   }
      // }

      // // 初始化固定长度的column family
      // datas_of_all_column_family_.back().fixed_length_column_family->init(
      //     column_lengths,
      //     "column_family_" + std::to_string(column_family_id) +
      //     ".property");
      // column_family_id++;

      // // 初始化动态长度的column family
      // if (string_mark) {
      //   datas_of_all_column_family_.back().dynamic_length_column_family->open(
      //       "column_family_" + std::to_string(column_family_id) +
      //           ".string_content",
      //       false);
      // }
    }
  }

  // FixedLengthColumnFamily& AddColumnFamily() {
  //   datas_of_all_column_family_.emplace_back(new FixedLengthColumnFamily(),
  //                                            nullptr, nullptr);
  //   return
  //   *(datas_of_all_column_family_.back().fixed_length_column_family);
  // }

  void InsertVertex(size_t vertex_id,
                    const std::vector<std::string_view>& values) {
    for (auto& column_to_column_family : column_to_column_familys_) {
      switch (column_to_column_family.column_type) {
      case ColumnType::kFixedLength: {
        datas_of_all_column_family_[column_to_column_family.column_family_id]
            .fixed_length_column_family->setColumn(
                vertex_id, column_to_column_family.column_id_in_column_family,
                values[column_to_column_family.property_id]);
        break;
      }
      case ColumnType::kDynamicString: {
        auto start_pos =
            gbp::as_atomic(datas_of_all_column_family_[column_to_column_family
                                                           .column_family_id]
                               .dynamic_length_column_family_size)
                .fetch_add(values[column_to_column_family.property_id].size());
        GetColumnFamily(column_to_column_family.column_family_id)
            .setColumn(vertex_id,
                       column_to_column_family.column_id_in_column_family,
                       values[column_to_column_family.property_id]);
        break;
      }
      case ColumnType::kDynamicEdgeList: {
        assert(false);
        break;
      }
      default: {
        assert(false);
      }
      }
    }
  }

  void Resize(size_t new_capacity_in_row) {
    for (auto& data_per_column_family : datas_of_all_column_family_) {
      data_per_column_family.fixed_length_column_family->resize(
          new_capacity_in_row);
    }
    capacity_in_row_ = new_capacity_in_row;
  }
  size_t CapacityInRow() const { return capacity_in_row_; }
  FixedLengthColumnFamily& GetColumnFamily(size_t column_family_id) {
    return *(datas_of_all_column_family_[column_family_id]
                 .fixed_length_column_family);
  }

 private:
  std::vector<DataPerColumnFamily> datas_of_all_column_family_;
  std::vector<ColumnToColumnFamily> column_to_column_familys_;

  size_t capacity_in_row_;
};
}  // namespace test
