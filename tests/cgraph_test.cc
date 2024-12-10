#include <yaml-cpp/yaml.h>
#include "tests.h"

namespace test {

void test_vertex(const std::string& config_file_path,
                 const std::string& data_file_path,
                 const std::string& db_dir_path) {
  gbp::DiskManager disk_manager;
  gbp::IOBackend* io_backend = new gbp::RWSysCall(&disk_manager);
  auto& bpm = gbp::BufferPoolManager::GetGlobalInstance();
  auto pool_num = 1;
  auto pool_size_page = 1024 * 1024;
  auto io_server_num = 1;
  bpm.init(pool_num, pool_size_page, io_server_num);

  // 加载配置文件
  YAML::Node graph_node = YAML::LoadFile(config_file_path);
  YAML::Node vertex_node;
  for (int i = 0; i < graph_node["schema"]["vertex_types"].size(); i++) {
    if (graph_node["schema"]["vertex_types"][i]["type_name"]
            .as<std::string>() == "PERSON") {
      vertex_node = graph_node["schema"]["vertex_types"][i];
      break;
    }
  }
  std::vector<std::vector<YAML::Node>> column_families;
  std::vector<bool> property_used(vertex_node["properties"].size(), false);
  for (int i = 0; i < vertex_node["properties"].size(); i++) {
    std::vector<YAML::Node> column_family;
    size_t column_family_id = std::numeric_limits<size_t>::max();
    for (int j = i; j < vertex_node["properties"].size(); j++) {
      if (!property_used[j]) {
        if (column_family_id == std::numeric_limits<size_t>::max()) {
          column_family_id =
              vertex_node["properties"][j]["column_family"].as<size_t>();
        } else if (column_family_id !=
                   vertex_node["properties"][j]["column_family"].as<size_t>()) {
          continue;
        }
        column_family.push_back(vertex_node["properties"][j]);
        property_used[j] = true;
      }
    }
    if (column_family.size() != 0)
      column_families.push_back(column_family);
  }
  std::cout << column_families.size() << std::endl;

  // 测试 CSV 加载
  csv::CSVLoader loader;
  if (!loader.load(data_file_path)) {
    std::cerr << "CSV文件加载失败" << std::endl;
    return;
  }

  Vertex person;

  // 初始化column family
  std::vector<std::vector<Vertex::ColumnConfiguration>>
      column_configurations;  // tuple<property_id, column_family_id,
                              // column_property_type,edge_type>
  for (int i = 0; i < column_families.size(); i++) {
    std::vector<Vertex::ColumnConfiguration> column_configurations_tmp;
    for (int j = 0; j < column_families[i].size(); j++) {
      auto column_type = Vertex::ColumnType::kFixedLength;
      auto column_width = 0;
      if (column_families[i][j]["property_type"]["primitive_type"]) {
        if (column_families[i][j]["property_type"]["primitive_type"]
                .as<std::string>() == "DT_SIGNED_INT64") {
          column_type = Vertex::ColumnType::kFixedLength;
          column_width = 8;  // 8 bytes
        } else if (column_families[i][j]["property_type"]["primitive_type"]
                       .as<std::string>() == "DT_STRING") {
          column_type = Vertex::ColumnType::kDynamicString;
          column_width = 16;  // 16 bytes
        } else {
          assert(false);
        }
      } else if (column_families[i][j]["property_type"]["date"]) {
        if (column_families[i][j]["property_type"]["date"].as<std::string>() ==
            "date") {
          column_type = Vertex::ColumnType::kFixedLength;
          column_width = 16;  // 16 bytes
        } else {
          assert(false);
        }
      } else {
        assert(false);
      }
      auto edge_type = Vertex::EdgeType::knone;
      column_configurations_tmp.emplace_back(
          column_families[i][j]["property_id"].as<size_t>(), i, column_type,
          edge_type, column_width);
    }
    column_configurations.push_back(column_configurations_tmp);
    // break;
    std::cout << std::endl;
  }
  person.init(column_configurations);
  // person.Resize(vertex_node["x_csr_params"]["max_vertex_num"].as<size_t>());
  // std::cout << "people.capacity_in_row_: " << person.CapacityInRow()
  //           << std::endl;

  // // bulkload
  // for (int i = 0; i < column_families.size(); i++) {
  //   auto& column_family = person.GetColumnFamily(i);
  //   for (int j = 0; j < column_families[i].size(); j++) {
  //     if (column_families[i][j]["property_type"]["primitive_type"]) {
  //       if (column_families[i][j]["property_type"]["primitive_type"]
  //               .as<std::string>() == "DT_SIGNED_INT64") {
  //       } else if (column_families[i][j]["property_type"]["primitive_type"]
  //                      .as<std::string>() == "DT_STRING") {
  //       } else {
  //         assert(false);
  //       }
  //     } else if (column_families[i][j]["property_type"]["date"]) {
  //       if (column_families[i][j]["property_type"]["date"].as<std::string>()
  //       ==
  //           "date") {
  //       } else {
  //         assert(false);
  //       }
  //     } else {
  //       assert(false);
  //     }
  //     auto column_id = column_families[i][j]["property_id"].as<size_t>();
  //     auto value = loader.get_column(column_id);
  //     for (int k = 0; k < value.size(); k++) {
  //       column_family.setColumn(k, j, value[k]);
  //     }
  //     std::cout << std::endl;
  //   }
  // }
}

void test_csv(const std::string& file_path) {
  // 测试 CSV 加载
  csv::CSVLoader loader;
  if (!loader.load(file_path)) {
    std::cerr << "CSV文件加载失败" << std::endl;
    return;
  }

  // 测试获取列名
  auto headers = loader.get_headers();
  std::cout << "CSV列名: ";
  for (const auto& header : headers) {
    std::cout << header << " ";
  }
  std::cout << std::endl;

  // 测试获取第一行
  auto first_row = loader.get_row(0);
  std::cout << "第一行数据: ";
  for (const auto& cell : first_row) {
    std::cout << cell << " ";
  }
  std::cout << std::endl;

  // 测试获取第一列
  auto first_column = loader.get_column(0);
  std::cout << "第一列数据: ";
  for (const auto& cell : first_column) {
    std::cout << cell << " ";
  }
  std::cout << std::endl;

  // 测试获取特定元素
  std::cout << "第1行第2列的元素: " << loader.get_element(0, 1) << std::endl;
}
}  // namespace test