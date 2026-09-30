// Intentional TSan detector control. This program must report a race.
#include <barrier>
#include <thread>

int counter = 0;

int main() {
  std::barrier start(3);
  auto work = [&] {
    start.arrive_and_wait();
    for (int i = 0; i < 10000; ++i) ++counter;
  };
  std::thread first(work);
  std::thread second(work);
  start.arrive_and_wait();
  first.join();
  second.join();
  return counter == 20000 ? 0 : 1;
}
