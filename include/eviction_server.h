#pragma once


#include <numa.h>
#include <pthread.h>
#include <xmmintrin.h>
#include <boost/circular_buffer.hpp>
#include <boost/fiber/all.hpp>
#include <cstddef>
#include <cstdint>
#include <mutex>
#include <optional>
#include <queue>
#include <stdexcept>
#include <thread>
#include <tuple>
#include <type_traits>
#include <vector>

#include <boost/lockfree/queue.hpp>

#include "config.h"
#include "io_backend.h"
#include "utils.h"
#include "replacer.h"
#include "page_table.h"
#include  "utils.h"

namespace gbp {
    using queue_lockfree_type = boost::lockfree::queue<mpage_id_type, boost::lockfree::fixed_sized<true>>;

    struct async_eviction_request_type {
        Replacer<mpage_id_type>* replacer;  // to find an unpinned page for replacement
        lockfree_queue_type<mpage_id_type>* free_list;     // to find a free page for replacement
        mpage_id_type page_num;
        std::atomic<bool>* finish;

        async_eviction_request_type(Replacer<mpage_id_type>* _replacer, lockfree_queue_type<mpage_id_type>* _free_list, mpage_id_type _page_num, std::atomic<bool>* _finish) {
            replacer = _replacer;
            free_list = _free_list;
            page_num = _page_num;
            finish = _finish;
        }
        ~async_eviction_request_type() = default;
    };

    class EvictionServer {
    public:
        EvictionServer() : stop_(false) {
            server_ = std::thread([this]() { Run(); });
        }
        ~EvictionServer() {
            stop_ = true;
            if (server_.joinable())
                server_.join();
        }

        bool SendRequest(Replacer<mpage_id_type>* replacer, lockfree_queue_type<mpage_id_type>* free_list, mpage_id_type page_num, std::atomic<bool>* finish, bool blocked = true) {
            auto* req = new async_eviction_request_type(replacer, free_list, page_num, finish);

            if (likely(blocked))
                while (!request_channel_.push(req));
            else { return request_channel_.push(req); }

            return true;
        }
    private:

        bool ProcessFunc(async_eviction_request_type& req) {
            mpage_id_type mpage_id;
            std::vector<mpage_id_type> pages;
            req.replacer->Victim(pages, req.page_num);
            for (auto& page : pages) {
                assert(req.free_list->Push(page));
                std::string tmp = "a" + std::to_string(page);
                // Log_mine(tmp);
            }
            req.page_num -= pages.size();
            if (unlikely(req.page_num == 0)) {
                return true;
            }

            return false;
        }

        void Run() {
            size_t loops = 100;
            boost::circular_buffer<std::optional<async_eviction_request_type*>>
                async_requests(gbp::FIBER_CHANNEL_DEPTH);

            async_eviction_request_type* async_request;
            while (true) {
                while (true == request_channel_.pop(async_request)) {
                    async_requests.push_back(async_request);
                    while (!async_requests.empty()) {
                        while (!async_requests.full() && true == request_channel_.pop(async_request))
                            async_requests.push_back(async_request);

                        for (auto& req : async_requests) {
                            if (!req.has_value())
                                continue;
                            if (ProcessFunc(*req.value())) {
                                req.value()->finish->store(true);
                                ::delete req.value();
                                req.reset();
                            }
                        }

                        while (!async_requests.empty() && !async_requests.front().has_value()) {
                            num_async_fiber_processing_--;
                            async_requests.pop_front();
                        }
                        // save_fence();
                        // boost::this_fiber::yield();
                    }
                }
                if (stop_) break;
                hybrid_spin(loops);
            }
        }

        std::thread server_;
        boost::lockfree::queue<async_eviction_request_type*, boost::lockfree::capacity<FIBER_CHANNEL_DEPTH>> request_channel_;
        size_t num_async_fiber_processing_;
        bool stop_;
    };
}