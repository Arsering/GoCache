// #include <yaml-cpp/yaml.h>

// #include "tests.h"

// namespace test {

// void test_vertex(const std::string& config_file_path,
//                  const std::string& data_file_path,
//                  const std::string& db_dir_path) {
//   gbp::DiskManager disk_manager;
//   gbp::IOBackend* io_backend = new gbp::RWSysCall(&disk_manager);
//   auto& bpm = gbp::BufferPoolManager::GetGlobalInstance();
//   auto pool_num = 1;
//   auto pool_size_page = 1024 * 1024;
//   auto io_server_num = 1;
//   bpm.init(pool_num, pool_size_page, io_server_num);

//   // 加载配置文件
//   YAML::Node graph_node = YAML::LoadFile(config_file_path);
//   YAML::Node vertex_node;
//   // 获取顶点person的配置信息
//   for (int i = 0; i < graph_node["schema"]["vertex_types"].size(); i++) {
//     if (graph_node["schema"]["vertex_types"][i]["type_name"]
//             .as<std::string>() == "PERSON") {
//       vertex_node = graph_node["schema"]["vertex_types"][i];
//       break;
//     }
//   }

//   std::vector<std::vector<YAML::Node>> column_families;
//   std::vector<bool> property_used(vertex_node["properties"].size(), false);
//   for (int i = 0; i < vertex_node["properties"].size(); i++) {
//     std::vector<YAML::Node> column_family;
//     size_t column_family_id = std::numeric_limits<size_t>::max();
//     for (int j = i; j < vertex_node["properties"].size(); j++) {
//       if (!property_used[j]) {
//         if (column_family_id == std::numeric_limits<size_t>::max()) {
//           column_family_id =
//               vertex_node["properties"][j]["column_family"].as<size_t>();
//         } else if (column_family_id !=
//                    vertex_node["properties"][j]["column_family"].as<size_t>())
//                    {
//           continue;
//         }
//         column_family.push_back(vertex_node["properties"][j]);
//         property_used[j] = true;
//       }
//     }
//     if (column_family.size() != 0)
//       column_families.push_back(column_family);
//   }

//   // 测试 CSV 加载
//   csv::CSVLoader loader;
//   if (!loader.load(data_file_path)) {
//     std::cerr << "CSV文件加载失败" << std::endl;
//     return;
//   }

//   Vertex person(2, db_dir_path);

//   // 初始化column family
//   std::vector<std::vector<Vertex::ColumnConfiguration>>
//   column_configurations; for (int i = 0; i < column_families.size(); i++) {
//     std::vector<Vertex::ColumnConfiguration> column_configurations_tmp;
//     for (int j = 0; j < column_families[i].size(); j++) {
//       auto column_type = Vertex::ColumnType::kDate;
//       if (column_families[i][j]["property_type"]["primitive_type"]) {
//         if (column_families[i][j]["property_type"]["primitive_type"]
//                 .as<std::string>() == "DT_SIGNED_INT64") {
//           column_type = Vertex::ColumnType::kInt64;
//         } else if (column_families[i][j]["property_type"]["primitive_type"]
//                        .as<std::string>() == "DT_STRING") {
//           column_type = Vertex::ColumnType::kDynamicString;
//         } else {
//           assert(false);
//         }
//       } else if (column_families[i][j]["property_type"]["date"]) {
//         if (column_families[i][j]["property_type"]["date"].as<std::string>()
//         ==
//             "date") {
//           column_type = Vertex::ColumnType::kDate;
//         } else {
//           assert(false);
//         }
//       } else {
//         assert(false);
//       }
//       column_configurations_tmp.emplace_back(
//           column_families[i][j]["property_id"].as<size_t>(), i, column_type,
//           Vertex::EdgeType::kNone);
//     }
//     column_configurations.push_back(column_configurations_tmp);
//     std::cout << std::endl;
//   }
//   column_configurations[0].emplace_back(
//       12, 0, Vertex::ColumnType::kDynamicEdgeList, Vertex::EdgeType::kEmpty);
//   column_configurations[1].emplace_back(
//       13, 1, Vertex::ColumnType::kDynamicEdgeList, Vertex::EdgeType::kDate);

//   // 初始化
//   person.Init(column_configurations);
//   // 设置最大顶点数
//   auto max_vertex_num =
//       vertex_node["x_csr_params"]["max_vertex_num"].as<size_t>();
//   person.Resize(max_vertex_num);
//   std::cout << "people.capacity_in_row_: " << person.CapacityInRow()
//             << std::endl;

//   // bulkload
//   size_t row_num = 100;
//   for (auto i = 0; i < column_configurations.size(); i++) {
//     for (auto j = 0; j < column_configurations[i].size() - 1; j++) {
//       auto property_id = column_families[i][j]["property_id"].as<size_t>();
//       auto value = loader.get_column(property_id);
//       switch (column_configurations[i][j].column_type) {
//       case Vertex::ColumnType::kDate: {
//         for (int k = 0; k < row_num; k++) {
//           uint64_t data = gbp::parseDateTimeToMilliseconds(value[k]);
//           gbp::GBPLOG << data;
//           person.InsertColumn(
//               k, {property_id,
//                   {reinterpret_cast<char*>(&data), sizeof(uint64_t)}});

//           auto result = person.ReadColumn(k, property_id);
//           assert(gbp::BufferBlock::Ref<uint64_t>(result) == data);
//         }
//         break;
//       }
//       case Vertex::ColumnType::kInt32: {
//         for (int k = 0; k < row_num; k++) {
//           int32_t data = std::stoi(value[k]);
//           gbp::GBPLOG << data;
//           person.InsertColumn(
//               k,
//               {property_id, {reinterpret_cast<char*>(&data),
//               sizeof(int32_t)}});
//           auto result = person.ReadColumn(k, property_id);
//           assert(gbp::BufferBlock::Ref<int32_t>(result) == data);
//         }
//         break;
//       }
//       case Vertex::ColumnType::kInt64: {
//         for (int k = 0; k < row_num; k++) {
//           int64_t data = std::stoll(value[k]);
//           gbp::GBPLOG << data;
//           person.InsertColumn(
//               k,
//               {property_id, {reinterpret_cast<char*>(&data),
//               sizeof(int64_t)}});
//           auto result = person.ReadColumn(k, property_id);
//           assert(gbp::BufferBlock::Ref<int64_t>(result) == data);
//         }
//         break;
//       }
//       case Vertex::ColumnType::kDynamicString: {
//         for (int k = 0; k < row_num; k++) {
//           std::string data = value[k];
//           gbp::GBPLOG << data;
//           person.InsertColumn(k, {property_id, data});
//           auto result = person.ReadColumn(k, property_id);
//           assert(result == data);
//         }
//         break;
//       }
//       case Vertex::ColumnType::kDynamicEdgeList: {
//         break;
//       }
//       default:
//         assert(false);
//       }
//     }
//   }

//   for (auto i = 0; i < column_configurations.size(); i++) {
//     auto column_configuration = column_configurations[i].back();
//     if (column_configuration.column_type ==
//         Vertex::ColumnType::kDynamicEdgeList) {
//       switch (column_configuration.edge_type) {
//       case Vertex::EdgeType::kEmpty: {
//         std::array<uint64_t, 3> edge_list = {10, 100, 299};
//         std::vector<std::pair<size_t, size_t>> edge_list_len;
//         for (auto vertex_id = 0; vertex_id < 100; vertex_id++) {
//           edge_list_len.push_back({vertex_id, edge_list.size()});
//         }
//         person.EdgeListInitBatch(column_configuration.property_id,
//                                  edge_list_len);
//         for (auto vertex_id = 0; vertex_id < 100; vertex_id++) {
//           for (auto edge : edge_list) {
//             MutableNbr<EmptyType> item;
//             item.neighbor = edge;
//             item.timestamp = 0;
//             person.InsertEdge(vertex_id, {column_configuration.property_id,
//                                           {reinterpret_cast<const
//                                           char*>(&item),
//                                            sizeof(MutableNbr<EmptyType>)}});
//           }
//           auto datas =
//               person.ReadEdgeList(vertex_id,
//               column_configuration.property_id);
//           for (auto edge_id = 0; edge_id < edge_list.size(); edge_id++) {
//             auto nbr =
//                 gbp::BufferBlock::Ref<MutableNbr<EmptyType>>(datas, edge_id)
//                     .neighbor;
//             if (nbr != edge_list[edge_id]) {
//               gbp::GBPLOG << "vertex_id: " << vertex_id
//                           << " edge_id: " << edge_id << " nbr: " << nbr
//                           << " edge_list[edge_id]: " << edge_list[edge_id];
//             }
//             assert(nbr == edge_list[edge_id]);
//           }
//           // return;
//         }
//         break;
//       }
//       case Vertex::EdgeType::kDate: {
//         std::array<uint64_t, 3> edge_list = {10, 100, 299};
//         std::vector<std::pair<size_t, size_t>> edge_list_len;
//         for (auto vertex_id = 0; vertex_id < 100; vertex_id++) {
//           edge_list_len.push_back({vertex_id, edge_list.size()});
//         }
//         person.EdgeListInitBatch(column_configuration.property_id,
//                                  edge_list_len);
//         for (auto vertex_id = 0; vertex_id < 100; vertex_id++) {
//           for (auto edge : edge_list) {
//             MutableNbr<uint64_t> item;
//             item.neighbor = edge;
//             item.timestamp = 0;
//             item.data = 100;
//             person.InsertEdge(vertex_id, {column_configuration.property_id,
//                                           {reinterpret_cast<const
//                                           char*>(&item),
//                                            sizeof(MutableNbr<uint64_t>)}});
//           }
//           auto datas =
//               person.ReadEdgeList(vertex_id,
//               column_configuration.property_id);
//           for (auto edge_id = 0; edge_id < edge_list.size(); edge_id++) {
//             auto nbr =
//                 gbp::BufferBlock::Ref<MutableNbr<uint64_t>>(datas, edge_id)
//                     .neighbor;
//             if (nbr != edge_list[edge_id]) {
//               gbp::GBPLOG << "vertex_id: " << vertex_id
//                           << " edge_id: " << edge_id << " nbr: " << nbr
//                           << " edge_list[edge_id]: " << edge_list[edge_id];
//             }

//             assert(nbr == edge_list[edge_id]);
//           }
//           // return;
//         }
//         break;
//       }
//       default:
//         assert(false);
//       }
//     } else {
//       assert(false);
//     }
//   }
// }

// void test_csv(const std::string& file_path) {
//   // 测试 CSV 加载
//   csv::CSVLoader loader;
//   if (!loader.load(file_path)) {
//     std::cerr << "CSV文件加载失败" << std::endl;
//     return;
//   }

//   // 测试获取列名
//   auto headers = loader.get_headers();
//   std::cout << "CSV列名: ";
//   for (const auto& header : headers) {
//     std::cout << header << " ";
//   }
//   std::cout << std::endl;

//   // 测试获取第一行
//   auto first_row = loader.get_row(0);
//   std::cout << "第一行数据: ";
//   for (const auto& cell : first_row) {
//     std::cout << cell << " ";
//   }
//   std::cout << std::endl;

//   // 测试获取第一列
//   auto first_column = loader.get_column(0);
//   std::cout << "第一列数据: ";
//   for (const auto& cell : first_column) {
//     std::cout << cell << " ";
//   }
//   std::cout << std::endl;

//   // 测试获取特定元素
//   std::cout << "第1行第2列的元素: " << loader.get_element(0, 1) << std::endl;
// }

// void test_graph(const std::string& config_file_path,
//                 const std::string& data_file_path,
//                 const std::string& db_dir_path) {
//   gbp::DiskManager disk_manager;
//   gbp::IOBackend* io_backend = new gbp::RWSysCall(&disk_manager);
//   auto& bpm = gbp::BufferPoolManager::GetGlobalInstance();
//   auto pool_num = 1;
//   auto pool_size_page = 1024 * 1024;
//   auto io_server_num = 1;
//   bpm.init(pool_num, pool_size_page, io_server_num);

//   // 加载配置文件
//   YAML::Node graph_node = YAML::LoadFile(config_file_path);
//   YAML::Node vertex_node;
//   // 获取顶点配置信息
//   for (int i = 0; i < graph_node["schema"]["vertex_types"].size(); i++) {
//     if (graph_node["schema"]["vertex_types"][i]["type_name"]
//             .as<std::string>() == "PERSON") {
//       vertex_node = graph_node["schema"]["vertex_types"][i];
//       break;
//     }
//   }
// }

// }  // namespace test