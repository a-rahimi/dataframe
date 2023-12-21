#include <chrono>
#include <string>

#include "dataframe.h"

struct Task {
    std::string task_id;
    std::string inquiry_id;
    std::string created_time_pt;
    std::string associate_id;
    std::string store_id;
    std::string store_format;
    int opportunities;
    float duration_of_task_seconds;
    float ttff;
    float buffer_time;
    float net_associate_effort;
    float task_latency_s;
    std::string task_queue;
    int quorum_size;
    int num_defects;
    int num_app_events;

    friend std::ostream& operator<<(std::ostream& s, const Task& t) {
        s << t.task_id << ", " << t.inquiry_id << ", " << t.created_time_pt << ", " << t.associate_id << ", "
          << t.store_id << ", " << t.store_format << ", " << t.opportunities << ", " << t.duration_of_task_seconds
          << ", " << t.ttff << ", " << t.buffer_time << t.net_associate_effort << ", " << t.task_latency_s << ", "
          << t.task_queue << ", " << t.quorum_size << ", " << t.num_defects << ", " << t.num_app_events << std::endl;
        return s;
    }
};

void from_tab_separated_string(Task& t, const std::string_view& s) {
    parse_tab_separated_string(s,
                               t.task_id,
                               t.inquiry_id,
                               t.created_time_pt,
                               t.associate_id,
                               t.store_id,
                               t.store_format,
                               t.opportunities,
                               t.duration_of_task_seconds,
                               t.ttff,
                               t.buffer_time,
                               t.net_associate_effort,
                               t.task_latency_s,
                               t.task_queue,
                               t.quorum_size,
                               t.num_defects,
                               t.num_app_events);
}

int main() {
    Timer timer;

    timer.start("Reading tab-separated file");
    auto tasks = read_tsv<Task>("3816f181-7751-4146-ae5e-43a7afdd9a37-0.tsv");
    timer.stop();

    std::cout << "read " << tasks.size() << " tasks" << std::endl;
    std::cout << "total size " << tasks.size() * sizeof(Task) / 1024 / 1024 << " MB.\n";
    std::cout << tasks[0].v;

    timer.start("retag expression");
    auto tasks_retagged = tasks.retag([](size_t, const Task& t) { return t.associate_id; });
    timer.stop();

    timer.start("Computing stats");
    auto NAET = *tasks_retagged.apply_to_values([](const Task& t) { return t.net_associate_effort; })
                     .reduce_moments()
                     .apply_to_values([](const auto& m) { return m.mean(); });
    timer.stop();

    //    std::cout << NAET;
}