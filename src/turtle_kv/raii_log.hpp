
#include <iostream>
#include <string>

class RAII_Log
{
 public:
  explicit RAII_Log(const std::string& message) : msg_(message)
  {
    std::cout << "Entering: " << msg_ << std::endl;
  }

  ~RAII_Log()
  {
    std::cout << "Leaving: " << msg_ << std::endl;
  }

 private:
  std::string msg_;
};