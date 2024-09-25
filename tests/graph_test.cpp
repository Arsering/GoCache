#include "tests.h"

#include "mmap_array_test.h"

namespace test {
int test_graph(int argc, char** argv) {
  size_t file_size_MB = std::stoull(argv[1]);
  size_t worker_num = std::stoull(argv[2]);
  size_t pool_num = std::stoull(argv[3]);
  size_t pool_size_MB = std::stoull(argv[4]);
  size_t io_server_num = std::stoull(argv[5]);
  size_t io_size = std::stoull(argv[6]);

  size_t pool_size_inpage =
      pool_size_MB * 1024LU * 1024LU / gbp::PAGE_SIZE_MEMORY / pool_num + 1;

  auto& bpm = gbp::BufferPoolManager::GetGlobalInstance();
  bpm.init(pool_num, pool_size_inpage, io_server_num);

  std::string person_first_name_file =
      "/nvme0n1/lgraph_db/sf30_db_t_t/snapshots/0/vertex_table_PERSON.col_0";
  std::string person_knows_person_deg_file =
      "/nvme0n1/lgraph_db/sf0.1_db_t_t/snapshots/0/oe_PERSON_KNOWS_PERSON.deg";
  std::string person_knows_person_nbr_file =
      "/nvme0n1/lgraph_db/sf0.1_db_t_t/snapshots/0/oe_PERSON_KNOWS_PERSON.nbr";

  mmap_array_test<std::string_view> first_name_string_view;
  mmap_array_test<int> person_knows_person_deg;
  mmap_array_test<int> person_knows_person_nbr;

  first_name_string_view.open(person_first_name_file, true);
  first_name_string_view.open(person_first_name_file, true);

  std::vector<char> str_buf(4096);
  gbp::GBPLOG << first_name_string_view.size();
  // for (size_t idx = 0; idx < first_name_string_view.size(); idx++) {
  //   auto block = first_name_string_view.get(idx);
  //   block.Copy(str_buf.data(), block.Size());

  //   gbp::GBPLOG << std::string_view{str_buf.data(), block.Size()};
  // }

  return 0;
}
}  // namespace test