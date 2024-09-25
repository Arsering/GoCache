#include <random>
#include <vector>
#include <cmath>
#include <iostream>
namespace test {

class ValueGenerator {
public:
    virtual void init(size_t io_num) = 0;
    virtual uint64_t get() = 0;
    virtual ~ValueGenerator() = default;
};

class RandomGenerator : public ValueGenerator {
private:
    std::mt19937 gen;
    std::uniform_int_distribution<uint64_t> rnd;
    
public:
    void init(size_t io_num) override {
        std::random_device rd;
        gen = std::mt19937(rd());
        rnd = std::uniform_int_distribution<uint64_t>(0, io_num);
    }
    
    uint64_t get() override {
        return rnd(gen);
    }
};

class PowerLawGenerator : public ValueGenerator {
private:
    std::mt19937 gen;
    std::piecewise_constant_distribution<double> power_law_dist;

public:
    void init(size_t io_num) override {
        const double alpha = 0.9; // 控制幂律分布的参数
        std::vector<double> intervals(io_num + 1);
        std::vector<double> weights(io_num + 1);
        for (size_t i = 0; i <= io_num; ++i) {
            intervals[i] = static_cast<double>(i);
            weights[i] = std::pow(i + 1, -alpha);
        }
        std::random_device rd;
        gen = std::mt19937(rd());
        power_law_dist = std::piecewise_constant_distribution<double>(intervals.begin(), intervals.end(), weights.begin());
    }

    uint64_t get() override {
        return static_cast<uint64_t>(power_law_dist(gen));
    }
};
}