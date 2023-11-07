#include <iostream>
#include <sys/file.h>
#include <sys/mman.h>
#include <unistd.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <time.h>
#include "buffer_pool_manager.h"

#include <ctime>
#include <random>

int test1()
{
    std::default_random_engine e;
    std::uniform_int_distribution<int> u(0, 100); // 左闭右闭区间
    e.seed(time(0));

    graphbuffer::page_id_infile temp_page_id;
    size_t pool_size = 1024LU * 1024LU;
    {
        graphbuffer::DiskManager *disk_manager = new graphbuffer::DiskManager("test.db");
        graphbuffer::BufferPoolManager bpm(pool_size, disk_manager);

        for (graphbuffer::page_id_infile page_num = 0; page_num < 100; page_num++)
        {
            auto page = bpm.NewPage(page_num);
            strcpy(page->GetData(), "Hello");
            page->SetDirty();
            if (!bpm.FlushPage(page_num))
            {
                std::cout << "failed" << std::endl;
                return -1;
            }
        }
    }
    graphbuffer::DiskManager *disk_manager = new graphbuffer::DiskManager("test.db");
    graphbuffer::BufferPoolManager bpm(10, disk_manager);

    for (int i = 0; i < 100; i++)
    {
        graphbuffer::page_id_infile page_num = i;
        // std::cout << page_num << std::endl;
        auto page = bpm.FetchPage(page_num);
        // std::cout << page->GetData()[0] << std::endl;
        bpm.UnpinPage(page);
    }
    return 0;
}

int test2()
{
    std::default_random_engine e;
    std::uniform_int_distribution<int> u(0, 100); // 左闭右闭区间
    e.seed(time(0));

    std::string another_file_name = "test1.db";

    graphbuffer::page_id_infile temp_page_id;
    size_t pool_size = 10;
    {
        graphbuffer::DiskManager *disk_manager = new graphbuffer::DiskManager("test.db");
        graphbuffer::BufferPoolManager bpm(pool_size, disk_manager);
        int file_handler = open(another_file_name.c_str(), O_RDWR | O_DIRECT | O_CREAT);
        file_handler = bpm.RegisterFile(file_handler);
        for (graphbuffer::page_id_infile page_num = 0; page_num < 100; page_num++)
        {
            auto page = bpm.NewPage(page_num);
            strcpy(page->GetData(), "Hello");
            page->SetDirty();

            if (!bpm.FlushPage(page_num))
            {
                std::cout << "failed" << std::endl;
                return -1;
            }
            bpm.UnpinPage(page);

            page = bpm.NewPage(page_num, file_handler);
            strcpy(page->GetData(), "Hello");
            page->SetDirty();
            if (!bpm.FlushPage(page_num, file_handler))
            {
                std::cout << "failed 1" << std::endl;
                return -1;
            }
            bpm.UnpinPage(page);
        }
    }
    std::cout << "Write test achieves success!!!" << std::endl;

    graphbuffer::DiskManager *disk_manager = new graphbuffer::DiskManager("test.db");
    graphbuffer::BufferPoolManager bpm(pool_size, disk_manager);

    for (int i = 0; i < 100; i++)
    {
        graphbuffer::page_id_infile page_num = i;
        // std::cout << page_num << std::endl;
        auto page = bpm.FetchPage(page_num);
        // std::cout << page->GetData()[0] << std::endl;
        bpm.UnpinPage(page);
    }
    std::cout << "Read test achieves success!!!" << std::endl;
    return 0;
}

int main()
{
    test2();
    return 0;
}