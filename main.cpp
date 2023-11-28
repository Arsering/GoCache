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
#include <string_view>
#include <assert.h>

#include "orgin_mmap.h"

template <typename T>
T *Decoder(void *data)
{
    return reinterpret_cast<T *>(data);
}

int test1()
{
    std::default_random_engine e;
    std::uniform_int_distribution<int> u(0, 100); // 左闭右闭区间
    e.seed(time(0));

    gbp::page_id_infile temp_page_id;
    size_t pool_size = 10;
    gbp::DiskManager *disk_manager = new gbp::DiskManager("test.db");
    gbp::BufferPoolManager *bpm = &gbp::BufferPoolManager::GetGlobalIntance();
    bpm->init(pool_size, disk_manager);

    {
        for (gbp::page_id_infile page_num = 0; page_num < 100; page_num++)
        {
            auto page = bpm->NewPage(page_num);
            strcpy(page->GetData(), "Hello");
            page->SetDirty();
            if (!bpm->FlushPage(page_num))
            {
                std::cout << "failed" << std::endl;
                return -1;
            }
        }
    }

    for (int i = 0; i < 100; i++)
    {
        gbp::page_id_infile page_num = i;
        // std::cout << page_num << std::endl;
        auto page = bpm->FetchPage(page_num);
        // std::cout << page->GetData()[0] << std::endl;
        bpm->ReleasePage(page);
    }
    return 0;
}

int test2()
{
    std::default_random_engine e;
    std::uniform_int_distribution<int> u(0, 100); // 左闭右闭区间
    e.seed(time(0));

    std::string another_file_name = "test1.db";

    gbp::page_id_infile temp_page_id;
    size_t pool_size = 10;
    gbp::DiskManager *disk_manager = new gbp::DiskManager("test.db");
    gbp::BufferPoolManager *bpm = &gbp::BufferPoolManager::GetGlobalIntance();
    bpm->init(pool_size, disk_manager);
    int file_handler = open(another_file_name.c_str(), O_RDWR | O_DIRECT | O_CREAT);
    file_handler = bpm->RegisterFile(file_handler);
    {

        for (gbp::page_id_infile page_num = 0; page_num < 100; page_num++)
        {
            auto page = bpm->NewPage(page_num);
            strcpy(page->GetData(), "Hello");
            page->SetDirty();

            if (!bpm->FlushPage(page_num))
            {
                std::cout << "failed" << std::endl;
                return -1;
            }
            bpm->ReleasePage(page);

            page = bpm->NewPage(page_num, file_handler);
            strcpy(page->GetData(), "Hello");
            page->SetDirty();
            if (!bpm->FlushPage(page_num, file_handler))
            {
                std::cout << "failed 1" << std::endl;
                return -1;
            }
            bpm->ReleasePage(page);
        }
    }
    std::cout << "Write test achieves success!!!" << std::endl;

    for (int i = 0; i < 100; i++)
    {
        gbp::page_id_infile page_num = i;
        // std::cout << page_num << std::endl;
        auto page = bpm->FetchPage(page_num);
        // std::cout << page->GetData()[0] << std::endl;
        bpm->ReleasePage(page);
    }
    std::cout << "Read test achieves success!!!" << std::endl;
    return 0;
}

class Student
{
    int age_;
    double credit_;

public:
    Student() = default;
    Student(int age, double credit) : age_(age), credit_(credit) {}

    ~Student() = default;

    int GetAge() const { return age_; }
    void SetAge(int age) { age_ = age; }
    double GetCredit() const { return credit_; }
    void SetCredit(double credit) { credit_ = credit; }
};

void test_mmap_array()
{
    gs::mmap_array<int> degree_list;
    degree_list.open("degree", true);
    assert(degree_list.size() == 10);
    for (int i = 0; i < degree_list.size(); i++)
    {
        assert(i + 1 == degree_list.get(i)->Obj<int>());
    }
    gs::mmap_array<Student> student_list;
    student_list.open("student", true);
    auto a = student_list.size();
    assert(a == 10);
    for (size_t i = 0; i < student_list.size(); i++)
    {
        auto s = student_list.get(i);
        Student &item = s->Obj<Student>();
        assert(item.GetAge() == i + 20);
        assert(item.GetCredit() == 3.2 + 0.1 * i);
    }
}

void generate_files()
{
    int degree[10] = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
    std::ofstream outFile("degree", std::ios::binary);
    if (outFile.is_open())
    {
        outFile.write(reinterpret_cast<char *>(degree), sizeof(int) * 10);
        outFile.close();
    }
    else
    {
        std::cerr << "failed to open the file for writing." << std::endl;
    }

    std::vector<Student> students(10);
    for (int i = 0; i < 10; i++)
    {
        students[i].SetAge(i + 20);
        students[i].SetCredit(3.2 + 0.1 * i);
    }
    std::ofstream outFile2("student", std::ios::binary);
    if (outFile2.is_open())
    {
        outFile2.write(reinterpret_cast<char *>(students.data()), sizeof(Student) * 10);
        outFile2.close();
    }
    else
    {
        std::cerr << "failed to open the file for writing." << std::endl;
    }
    std::cout << "generate files success!" << std::endl;
}

void test_string()
{
    gs::mmap_array<std::string_view> student_list;
    student_list.open("test_string", true);
    auto a = student_list.size();
    printf("begin a is %d\n", a);
    for (int i = 0; i < student_list.size(); i++)
    {
        auto s = student_list.get(i);
        std::string ss(s->Data(), s->Size());
        std::cout << ss << std::endl;
    }
}

void generate_string()
{
    gs::string_item item_list[10];
    uint64_t offset_list[10];
    uint32_t length_list[10];
    offset_list[0] = 0;
    length_list[0] = 5;
    for (int i = 1; i < 10; i++)
    {
        offset_list[i] = offset_list[i - 1] + length_list[i - 1];
        length_list[i] = 5 + i;
    }
    char *data = (char *)malloc(sizeof(char) * (offset_list[9] + length_list[9]));
    std::cout << "check point" << std::endl;
    for (int i = 0; i < 10; i++)
    {
        item_list[i].length = length_list[i];
        item_list[i].offset = offset_list[i];
        for (int j = offset_list[i]; j < offset_list[i] + length_list[i]; j++)
        {
            data[j] = 'A' + i;
        }
        std::string ss(data + offset_list[i], length_list[i]);
        std::cout << ss << std::endl;
    }
    std::ofstream outFile3("test_string.items", std::ios::binary);
    if (outFile3.is_open())
    {
        outFile3.write(reinterpret_cast<char *>(item_list), sizeof(gs::string_item) * 10);
        outFile3.close();
    }
    else
    {
        std::cerr << "failed to open the file for writing." << std::endl;
    }
    std::ofstream outFile4("test_string.data", std::ios::binary);
    if (outFile4.is_open())
    {
        outFile4.write(reinterpret_cast<char *>(data), sizeof(char) * (offset_list[9] + length_list[9]));
        outFile4.close();
    }
    else
    {
        std::cerr << "failed to open the file for writing." << std::endl;
    }
    std::cout << "generate_string success!!!" << std::endl;
}

int test_atomic()
{
    class MutableNbr
    {
    public:
        MutableNbr() = default;
        MutableNbr(const MutableNbr &rhs)
            : neighbor(rhs.neighbor),
              timestamp(rhs.timestamp.load()),
              data(rhs.data) {}
        MutableNbr &operator=(const MutableNbr &rhs)
        {
            this->neighbor = rhs.neighbor;
            this->timestamp.store(rhs.timestamp.load());
            this->data = rhs.data;
            return *this;
        }
        void init(size_t neighbor, size_t timestamp, size_t data)
        {
            this->neighbor = neighbor;
            this->timestamp.store(timestamp);
            this->data = data;
        }
        void print() const
        {
            std::cout << "neighbor: " << neighbor << " timestamp: " << timestamp << " data: " << data << std::endl;
        }
        ~MutableNbr() = default;

    private:
        size_t neighbor;
        std::atomic<size_t> timestamp;
        size_t data;
    };
    MutableNbr *buf = (MutableNbr *)malloc(sizeof(MutableNbr) * 10);
    MutableNbr item;
    item.init(14, 16, 10);

    memcpy(buf, &item, sizeof(MutableNbr));
    buf->print();
}

template <typename T>
class Test
{
private:
    T data_;

public:
    Test() = default;
    ~Test() = default;
    void set(T &data)
    {
        data_ = data;
    }
    T printa()
    {
        return data_;
    }
};
template <>
class Test<int>
{
private:
    int data_;

public:
    Test() = default;
    Test(const Test &other) : data_(other.data_)
    {
        std::cout << "MyClass Copy Ctor!" << std::endl;
    }
    ~Test() = default;
    void set(int &data)
    {
        data_ = data;
    }
    char printa()
    {
        return 'a';
    }
};
template <typename T>
Test<T> test_template(T &data)
{
    Test<T> result;
    result.set(data);
    // Test<T> aa(result);
    return result;
}
Test<int> returnaa()
{
    int a = 10;
    auto aa = test_template(a);
    return aa;
}
int test_shared()
{
    char *buf = (char *)malloc(1024LU * 1024LU * 1024LU * 10lU);
    {
        std::shared_ptr<Test<int>> test(reinterpret_cast<Test<int> *>(buf), free);
        int a = 10;
        test->set(a);
    }
    while (true)
    {
    }
    std::cout << buf << std::endl;
    return 0;
}
int main()
{
    // graphbuffer::DiskManager *disk_manager = new graphbuffer::DiskManager("test.db");
    size_t pool_size = 1000;
    gbp::BufferPoolManager *bpm = &gbp::BufferPoolManager::GetGlobalIntance();
    bpm->init(pool_size);
    // test2();

    generate_files();
    test_mmap_array();
    generate_string();
    test_string();

    std::cout
        << "Read test achieves success!!!" << std::endl;
    return 0;
}