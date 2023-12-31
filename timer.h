#include <chrono>
#include <iostream>

struct Timer {
    std::chrono::time_point<std::chrono::high_resolution_clock> t_start;
    std::string context;

    void start(const std::string &_context) {
        context = _context;
        t_start = std::chrono::high_resolution_clock::now();
    }
    void stop() {
        auto t_stop = std::chrono::high_resolution_clock::now();
        std::cout << context << ": " << std::chrono::duration_cast<std::chrono::milliseconds>(t_stop - t_start).count()
                  << " ms.\n";
    }
};
