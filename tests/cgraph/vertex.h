#include <vector>

#include "../../include/mmap_array.h"
#include "column.h"
#include "column_family.h"
#include "mutable_csr.h"

namespace test {

class Vertex {
 public:
  enum class ColumnType {
    kInt32,
    kDate,
    kString,
    kEmpty,
    kInt64,
    kDouble,
    KEdge,
    kDynamicString,
    kDynamicEdgeList,
  };

  enum class EdgeStrategy {
    kNone,
    kSingle,
    kMultiple,
  };

  enum class EdgeType {
    kInt32,
    kDate,
    kNone,
    kEmpty,
  };
  struct ColumnConfiguration {
   public:
    ColumnConfiguration(size_t property_id, size_t column_family_id,
                        ColumnType column_type, EdgeType edge_type)
        : property_id(property_id),
          column_family_id(column_family_id),
          column_type(column_type),
          edge_type(edge_type) {}
    size_t property_id;
    size_t column_family_id;
    ColumnType column_type;
    EdgeType edge_type;
  };

  class DataPerColumnFamily {
   public:
    DataPerColumnFamily() {
      fixed_length_column_family = new FixedLengthColumnFamily();
      dynamic_length_column_family = new mmap_array<char>();
      dynamic_length_column_family_size = 0;
    }
    ~DataPerColumnFamily() {
      if (fixed_length_column_family != nullptr) {
        delete fixed_length_column_family;
        fixed_length_column_family = nullptr;
      }
      if (dynamic_length_column_family != nullptr) {
        delete dynamic_length_column_family;
        dynamic_length_column_family = NULL;
      }
      for (auto& csr : csr) {
        delete csr.second;
      }
    }
    FixedLengthColumnFamily* fixed_length_column_family = nullptr;
    mmap_array<char>* dynamic_length_column_family = nullptr;
    size_t dynamic_length_column_family_size = 0;
    std::vector<std::pair<size_t, mmap_array_base*>>
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

  Vertex(size_t column_family_num, const std::string& db_dir_path) {
    column_family_num_ = column_family_num;
    datas_of_all_column_family_.reserve(column_family_num);

    db_dir_path_ = db_dir_path;
  }
  ~Vertex() = default;
  // tuple<property_id, column_family_id, column_type, edge_type>
  void Init(const std::vector<std::vector<ColumnConfiguration>>&
                column_configurations) {
    auto column_family_id = 0;
    auto column_id = 0;
    for (auto& column_configurations_tmp : column_configurations) {
      // 初始化每一个Column Family
      std::vector<size_t> column_lengths;  // 每个column的长度
      auto string_mark = false;            // 是否有string类型的column
      datas_of_all_column_family_.emplace_back();

      // 初始化每一个Column
      auto edge_list_column_id = 0;
      auto column_id_in_column_family = 0;  // 在column family中的id
      for (auto& column_configuration : column_configurations_tmp) {
        // 获取column的类型
        column_to_column_familys_.emplace_back(
            column_configuration.property_id, column_configuration.column_type,
            column_family_id, std::numeric_limits<size_t>::max(),
            column_id_in_column_family++);

        // 将property_id和column_to_column_family的index进行映射
        if (property_id_to_ColumnToColumnFamily_.count(
                column_configuration.property_id) == 0) {
          property_id_to_ColumnToColumnFamily_[column_configuration
                                                   .property_id] =
              column_to_column_familys_.size() - 1;
        } else {
          assert(false);
        }

        // 根据column的类型，初始化column family
        switch (column_configuration.column_type) {
        case ColumnType::kInt32:
          column_lengths.push_back(sizeof(int32_t));
          break;
        case ColumnType::kDate:
          column_lengths.push_back(sizeof(uint64_t));
          break;
        case ColumnType::kInt64: {
          column_lengths.push_back(sizeof(int64_t));
          break;
        }
        case ColumnType::kDynamicString: {
          column_lengths.push_back(sizeof(string_item));
          string_mark = true;
          break;
        }
        case ColumnType::kDynamicEdgeList: {
          column_lengths.push_back(sizeof(MutableAdjlist));
          switch (column_configuration.edge_type) {
          case EdgeType::kInt32: {
            auto mmap_array_ptr = new mmap_array<MutableNbr<int32_t>>();
            mmap_array_ptr->open(db_dir_path_ + "/column_family_" +
                                     std::to_string(column_family_id) + "_" +
                                     std::to_string(edge_list_column_id) +
                                     ".edgelist",
                                 false);
            datas_of_all_column_family_[column_family_id].csr.emplace_back(
                0, mmap_array_ptr);
            break;
          }
          case EdgeType::kDate: {
            auto mmap_array_ptr = new mmap_array<MutableNbr<uint64_t>>();
            mmap_array_ptr->open(db_dir_path_ + "/column_family_" +
                                     std::to_string(column_family_id) + "_" +
                                     std::to_string(edge_list_column_id) +
                                     ".edgelist",
                                 false);
            datas_of_all_column_family_[column_family_id].csr.emplace_back(
                0, mmap_array_ptr);
            break;
          }
          case EdgeType::kNone: {
            auto mmap_array_ptr = new mmap_array<MutableNbr<EmptyType>>();
            mmap_array_ptr->open(db_dir_path_ + "/column_family_" +
                                     std::to_string(column_family_id) + "_" +
                                     std::to_string(edge_list_column_id) +
                                     ".edgelist",
                                 false);
            datas_of_all_column_family_[column_family_id].csr.emplace_back(
                0, mmap_array_ptr);
            break;
          }
          case EdgeType::kEmpty: {
            auto mmap_array_ptr = new mmap_array<MutableNbr<EmptyType>>();
            mmap_array_ptr->open(db_dir_path_ + "/column_family_" +
                                     std::to_string(column_family_id) + "_" +
                                     std::to_string(edge_list_column_id) +
                                     ".edgelist",
                                 false);
            datas_of_all_column_family_[column_family_id].csr.emplace_back(
                0, mmap_array_ptr);
            break;
          }
          default: {
            assert(false);
          }
          }
          column_to_column_familys_.back().edge_list_id = edge_list_column_id++;
          break;
        }
        }
      }
      // 初始化固定长度的column family
      datas_of_all_column_family_[column_family_id]
          .fixed_length_column_family->init(
              column_lengths, db_dir_path_ + "/column_family_" +
                                  std::to_string(column_family_id) +
                                  ".property");

      // 初始化存储string的文件
      if (string_mark) {
        datas_of_all_column_family_[column_family_id]
            .dynamic_length_column_family->open(
                db_dir_path_ + "/column_family_" +
                    std::to_string(column_family_id) + ".string_content",
                false);
      }
      column_family_id++;
    }
  }

  void InsertVertex(
      size_t vertex_id,
      const std::vector<std::pair<size_t, std::string_view>>& values) {
    for (auto& value : values) {
      InsertColumn(vertex_id, value);
    }
  }

  void InsertColumn(size_t vertex_id,
                    const std::pair<size_t, std::string_view> value) {
    auto column_to_column_family = column_to_column_familys_
        [property_id_to_ColumnToColumnFamily_[value.first]];
    switch (column_to_column_family.column_type) {
    case ColumnType::kInt32:
    case ColumnType::kDate:
    case ColumnType::kInt64: {
      datas_of_all_column_family_[column_to_column_family.column_family_id]
          .fixed_length_column_family->setColumn(
              vertex_id, column_to_column_family.column_id_in_column_family,
              value.second);
      break;
    }
    case ColumnType::kDynamicString: {
      // 获得start_pos
      auto start_pos =
          gbp::as_atomic(datas_of_all_column_family_[column_to_column_family
                                                         .column_family_id]
                             .dynamic_length_column_family_size)
              .fetch_add(value.second.size());

      // 存储string内容
      datas_of_all_column_family_[column_to_column_family.column_family_id]
          .dynamic_length_column_family->set(start_pos, value.second,
                                             value.second.size());

      // 存储string的position
      string_item position = {start_pos, value.second.size()};
      datas_of_all_column_family_[column_to_column_family.column_family_id]
          .fixed_length_column_family->setColumn(
              vertex_id, column_to_column_family.column_id_in_column_family,
              {reinterpret_cast<char*>(&position), sizeof(string_item)});
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
  void EdgeListInitBatch(size_t column_id,
                         std::vector<std::pair<size_t, size_t>>& values) {
    auto column_to_column_family = column_to_column_familys_
        [property_id_to_ColumnToColumnFamily_[column_id]];
    for (auto& value : values) {
      auto item_t =
          datas_of_all_column_family_[column_to_column_family.column_family_id]
              .fixed_length_column_family->getColumn(
                  value.first,
                  column_to_column_family.column_id_in_column_family);
      gbp::BufferBlock::UpdateContent<MutableAdjlist>(
          [&](MutableAdjlist& item) {
            // 获得锁
            assert(item.start_idx_ == 0);
            assert(item.capacity_ == 0);
            assert(item.size_ == 0);
            item.start_idx_ =
                datas_of_all_column_family_[column_to_column_family
                                                .column_family_id]
                    .csr[column_to_column_family.edge_list_id]
                    .first;
            item.capacity_ = value.second;
            item.size_ = 0;

            datas_of_all_column_family_[column_to_column_family
                                            .column_family_id]
                .csr[column_to_column_family.edge_list_id]
                .first += value.second;
          },
          item_t);
    }
  }
  // pair<property_id, value>
  void InsertEdge(size_t vertex_id, std::pair<size_t, std::string_view> value) {
    auto column_to_column_family = column_to_column_familys_
        [property_id_to_ColumnToColumnFamily_[value.first]];
    if (column_to_column_family.column_type == ColumnType::kDynamicEdgeList) {
      auto item_t =
          datas_of_all_column_family_[column_to_column_family.column_family_id]
              .fixed_length_column_family->getColumn(
                  vertex_id,
                  column_to_column_family.column_id_in_column_family);
      size_t idx_new = 0;
      gbp::BufferBlock::UpdateContent<MutableAdjlist>(
          [&](MutableAdjlist& item) {
            // 获得锁
            u_int16_t old_data = 0;
            while (item.lock_.compare_exchange_weak(old_data, 1,
                                                    std::memory_order_release,
                                                    std::memory_order_relaxed))
              ;
            idx_new = item.size_.fetch_add(1);
            if (item.capacity_ <= idx_new) {
              assert(false);  // 没有空闲空间，需要重新分配
            }
            idx_new += item.start_idx_;
          },
          item_t);
      // 插入边
      datas_of_all_column_family_[column_to_column_family.column_family_id]
          .csr[column_to_column_family.edge_list_id]
          .second->set_single_obj(idx_new, value.second);
      gbp::BufferBlock::UpdateContent<MutableAdjlist>(
          [&](MutableAdjlist& item) { item.lock_.store(0); },
          item_t);  // 释放锁
    } else {
      assert(false);
    }
  }

  void InsertEdgeList(size_t vertex_id,
                      std::pair<size_t, std::vector<std::string_view>&> value) {
    auto column_to_column_family = column_to_column_familys_
        [property_id_to_ColumnToColumnFamily_[value.first]];
    if (column_to_column_family.column_type == ColumnType::kDynamicEdgeList) {
      auto item_t =
          datas_of_all_column_family_[column_to_column_family.column_family_id]
              .fixed_length_column_family->getColumn(
                  vertex_id,
                  column_to_column_family.column_id_in_column_family);
      size_t idx_new = 0;
      gbp::BufferBlock::UpdateContent<MutableAdjlist>(
          [&](MutableAdjlist& item) {
            // 获得锁
            u_int16_t old_data = 0;
            while (item.lock_.compare_exchange_weak(old_data, 1,
                                                    std::memory_order_release,
                                                    std::memory_order_relaxed))
              ;
            idx_new = item.size_.fetch_add(value.second.size());
            if (item.capacity_ <= idx_new) {
              assert(false);  // 没有空闲空间，需要重新分配
            }
            idx_new += item.start_idx_;
          },
          item_t);
      // 插入边
      for (auto& item : value.second) {
        datas_of_all_column_family_[column_to_column_family.column_family_id]
            .csr[column_to_column_family.edge_list_id]
            .second->set_single_obj(idx_new, item);
        idx_new++;
      }
      gbp::BufferBlock::UpdateContent<MutableAdjlist>(
          [&](MutableAdjlist& item) { item.lock_.store(0); },
          item_t);  // 释放锁
    } else {
      assert(false);
    }
  }

  gbp::BufferBlock ReadEdgeList(size_t vertex_id, size_t property_id) {
    auto column_to_column_family = column_to_column_familys_
        [property_id_to_ColumnToColumnFamily_[property_id]];
    if (column_to_column_family.column_type != ColumnType::kDynamicEdgeList) {
      assert(false);
    }
    auto item_t =
        datas_of_all_column_family_[column_to_column_family.column_family_id]
            .fixed_length_column_family->getColumn(
                vertex_id, column_to_column_family.column_id_in_column_family);
    auto& item = gbp::BufferBlock::Ref<MutableAdjlist>(item_t);
    return datas_of_all_column_family_[column_to_column_family.column_family_id]
        .csr[column_to_column_family.edge_list_id]
        .second->get(item.start_idx_, item.size_);
  }

  // std::vector<std::pair<size_t, std::string_view>> void ReadVertex(
  //     size_t vertex_id) {
  //   for (auto& value : values) {
  //     InsertColumn(vertex_id, value);
  //   }
  // }

  gbp::BufferBlock ReadColumn(size_t vertex_id, size_t property_id) {
#if ASSERT_ENABLE
    // assert(vertex_id < row_capacity_);
    assert(property_id_to_ColumnToColumnFamily_.count(property_id) == 1);
#endif
    gbp::BufferBlock result;
    auto column_to_column_family = column_to_column_familys_
        [property_id_to_ColumnToColumnFamily_[property_id]];
    switch (column_to_column_family.column_type) {
    case ColumnType::kInt32:
    case ColumnType::kDate:
    case ColumnType::kInt64: {
      result =
          datas_of_all_column_family_[column_to_column_family.column_family_id]
              .fixed_length_column_family->getColumn(
                  vertex_id,
                  column_to_column_family.column_id_in_column_family);
      break;
    }
    case ColumnType::kDynamicString: {
      // 存储string的position
      // string_item position = {start_pos, value.second.size()};
      auto position =
          datas_of_all_column_family_[column_to_column_family.column_family_id]
              .fixed_length_column_family->getColumn(
                  vertex_id,
                  column_to_column_family.column_id_in_column_family);

      auto& item = gbp::BufferBlock::Ref<string_item>(position);
      result =
          datas_of_all_column_family_[column_to_column_family.column_family_id]
              .dynamic_length_column_family->get(item.offset, item.length);
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
    return result;
  }

  void Resize(size_t new_capacity_in_row) {
    for (auto& data_per_column_family : datas_of_all_column_family_) {
      data_per_column_family.fixed_length_column_family->resize(
          new_capacity_in_row);
      data_per_column_family.dynamic_length_column_family->resize(
          new_capacity_in_row * 4096 * 1);
      for (auto& csr : data_per_column_family.csr) {
        csr.second->resize(new_capacity_in_row * 1024);
      }
    }
    for (auto& column_to_column_family : column_to_column_familys_) {
      if (column_to_column_family.column_type == ColumnType::kDynamicEdgeList) {
        for (size_t row_id = capacity_in_row_; row_id < new_capacity_in_row;
             row_id++) {
          auto item_t =
              datas_of_all_column_family_[column_to_column_family
                                              .column_family_id]
                  .fixed_length_column_family->getColumn(
                      row_id,
                      column_to_column_family.column_id_in_column_family);

          gbp::BufferBlock::UpdateContent<MutableAdjlist>(
              [&](MutableAdjlist& item) {
                item.capacity_ = 0;
                item.start_idx_ =
                    datas_of_all_column_family_[column_to_column_family
                                                    .column_family_id]
                        .csr[column_to_column_family.edge_list_id]
                        .first;
                item.size_ = 0;
              },
              item_t);
        }
      }
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
  std::map<size_t, size_t> property_id_to_ColumnToColumnFamily_;
  size_t column_family_num_;
  size_t capacity_in_row_ = 0;
  std::string db_dir_path_;
};
}  // namespace test
