#include <sys/mman.h>
#include <assert.h>
#include <unistd.h>
#include <cstddef>
#include <vector>

namespace gbp{
class VMCache{
  constexpr static size_t FILE_PAGE_SIZE = 4096;
  const int FLAGS = MAP_ANONYMOUS|MAP_PRIVATE|MAP_NORESERVE;
  const int PROT = PROT_READ | PROT_WRITE;
  public:
  /**
   * @brief 
   * @param vmSize 虚拟内存大小(单位:页数)
   */
    VMCache(size_t vmSize):vmSize_(vmSize){
      virtMem_ = (char*)mmap(0, vmSize, PROT, FLAGS, -1, 0);
      assert(virtMem_ != MAP_FAILED);
      pageTable_.resize(vmSize, 0);
    }

    void load(size_t offset){
      auto ret = ::pread(0, (void*) (virtMem_+offset), FILE_PAGE_SIZE, offset);
      assert(ret != -1);
      pageTable_[offset] = 1;
    }
    void evict(size_t offset){
      auto ret = ::pwrite(0, (void*) (virtMem_+offset), FILE_PAGE_SIZE, offset);
      assert(ret != -1);
      pageTable_[offset] = 0;
    }
    
    void registerFile(){assert(false);}
    static VMCache& GetGlobalInstance(){
      size_t vmSize = 1024*1024*10;
      static VMCache vmCache(vmSize);
      return vmCache;
    }

  private:
    char* virtMem_;
    size_t vmSize_;

    std::vector<size_t> pageTable_;

};
}  // namespace gbp