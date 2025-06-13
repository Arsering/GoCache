 #pragma once

#include <string>
#include <mutex>
#include <sstream>
#include <fstream>
#include <unistd.h>

namespace gbp {

// 页面错误统计结构
struct PageFaultStats {
    long minor_faults;      // 次页面错误
    long major_faults;      // 主页面错误
    long total_faults() const { return minor_faults + major_faults; }
    
    PageFaultStats(long minor = 0, long major = 0) 
        : minor_faults(minor), major_faults(major) {}
    
    PageFaultStats operator-(const PageFaultStats& other) const {
        return PageFaultStats(minor_faults - other.minor_faults, 
                            major_faults - other.major_faults);
    }
};

// 页面错误监控器类
class PageFaultMonitor {
private:
    std::string stat_path_;
    PageFaultStats last_stats_;
    mutable std::mutex mutex_;

    PageFaultStats parseCurrentStats() const {
        std::ifstream file(stat_path_);
        if (!file.is_open()) return PageFaultStats();

        std::string line;
        if (!std::getline(file, line)) return PageFaultStats();

        std::istringstream iss(line);
        std::string token;
        long minor = 0, major = 0;
        
        for (int i = 0; i < 13 && iss >> token; ++i) {
            if (i == 9) minor = std::stol(token);
            else if (i == 11) major = std::stol(token);
        }
        
        return PageFaultStats(minor, major);
    }

public:
    explicit PageFaultMonitor(pid_t pid = 0) {
        pid_t target_pid = (pid == 0) ? getpid() : pid;
        stat_path_ = "/proc/" + std::to_string(target_pid) + "/stat";
        reset();
    }

    PageFaultStats getDelta() {
        std::lock_guard<std::mutex> lock(mutex_);
        auto current = parseCurrentStats();
        auto delta = current - last_stats_;
        last_stats_ = current;
        return delta;
    }

    void reset() {
        std::lock_guard<std::mutex> lock(mutex_);
        last_stats_ = parseCurrentStats();
    }

    PageFaultStats getCurrent() const {
        return parseCurrentStats();
    }

    static std::string format(const PageFaultStats& stats) {
        std::ostringstream oss;
        oss << "PF[" << stats.minor_faults << "m+" << stats.major_faults 
            << "M=" << stats.total_faults() << "]";
        return oss.str();
    }
};

inline PageFaultMonitor& getGlobalPageFaultMonitor() {
    static PageFaultMonitor monitor;
    return monitor;
}

inline PageFaultStats getPageFaultDelta() {
    return getGlobalPageFaultMonitor().getDelta();
}

inline void resetPageFaultBaseline() {
    getGlobalPageFaultMonitor().reset();
}

inline std::string formatPageFaultStats(const PageFaultStats& stats) {
    return PageFaultMonitor::format(stats);
}

inline PageFaultStats getCurrentPageFaultStats() {
    return getGlobalPageFaultMonitor().getCurrent();
}

} //