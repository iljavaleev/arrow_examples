#pragma once

#include <chrono>
#include <iostream>

struct timer 
{
  timer() : start_{std::chrono::system_clock::now()} {}
  ~timer() 
  {
    std::cout << std::chrono::duration<double>(
                     std::chrono::system_clock::now() - start_)
                     .count()
              << " s\n";
  }

  std::chrono::time_point<std::chrono::system_clock> start_;
};