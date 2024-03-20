#include "../include/utils.h"
namespace gbp {
    namespace tools {}
    void Log_mine(std::string& content) {
        static std::mutex latch;
        std::lock_guard lock(latch);
        std::cout << content << std::endl;
        return;
    }
}  // namespace gbp