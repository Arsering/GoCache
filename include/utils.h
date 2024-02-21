namespace gbp {
namespace tools {
class QueryDecoder {
 public:
  QueryDecoder(const char* ptr, size_t size) : data_(ptr), end_(ptr + size) {}
  ~QueryDecoder() {}

 private:
  const char* data_;
  const char* end_;
};
class QueryEncoder {
 public:
  Encoder(std::vector<char>& buf) : buf_(buf) {}

  void put_long(int64_t v);

  void put_string(const std::string& v);

  void put_string_view(const std::string_view& v);

 private:
  std::vector<char>& buf_;
};
}  // namespace tools
}  // namespace gbp