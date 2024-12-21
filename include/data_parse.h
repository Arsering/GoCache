#pragma once

#include <boost/date_time/posix_time/posix_time.hpp>
#include <boost/date_time/posix_time/posix_time_io.hpp>
#include <iostream>
#include <string>

namespace gbp {
// size_t parseDateTimeToMilliseconds(const std::string& datetime) {
//   try {
//     // 定义时间格式
//     std::string format1 = "%Y-%m-%d";
//     std::string format2 = "%Y-%m-%dT%H:%M:%S.%f%q";

//     // 创建时间输入流
//     std::istringstream ss(datetime);
//     boost::posix_time::ptime pt;

//     // 尝试解析完整的日期时间格式
//     ss.imbue(std::locale(std::locale::classic(),
//                          new boost::posix_time::time_input_facet(format2)));
//     ss >> pt;
//     if (pt.is_not_a_date_time()) {
//       // 重置流状态并尝试解析仅日期格式
//       ss.clear();
//       ss.str(datetime);
//       ss.imbue(std::locale(std::locale::classic(),
//                            new
//                            boost::posix_time::time_input_facet(format1)));
//       ss >> pt;
//     }

//     if (pt.is_not_a_date_time()) {
//       throw std::runtime_error("Failed to parse date time");
//     }

//     // 计算自 Unix 时间以来的毫秒数
//     boost::posix_time::ptime epoch(boost::gregorian::date(1970, 1, 1));
//     boost::posix_time::time_duration diff = pt - epoch;
//     return diff.total_milliseconds();
//   } catch (const std::exception& e) {
//     std::cerr << "Error: " << e.what() << std::endl;
//     return -1;
//   }
// }

// int test_date_parse() {
//   try {
//     std::string datetime1 = "1981-08-28";
//     int64_t milliseconds1 = parseDateTimeToMilliseconds(datetime1);
//     std::cout << "Milliseconds since Unix epoch for " << datetime1 << ": "
//               << milliseconds1 << std::endl;

//     std::string datetime2 = "2012-03-23T00:44:07.183+0000";
//     int64_t milliseconds2 = parseDateTimeToMilliseconds(datetime2);
//     std::cout << "Milliseconds since Unix epoch for " << datetime2 << ": "
//               << milliseconds2 << std::endl;
//   } catch (const std::exception& e) {
//     std::cerr << "Error: " << e.what() << std::endl;
//   }

//   return 0;
// }
}  // namespace gbp