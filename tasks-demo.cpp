#include <string>

#include "dataframe.h"

struct Task {
    /*
   task_id	inquiry_id	created_time_pt	associate_id	store_id	store_format	opportunities
duration_of_task_seconds	ttff	buffer_time	net_associate_effort	task_latency_s	task_queue	quorum_size
num_defects	num_app_events

e09f4129-f5e1-4705-b0f5-2b051e5dee90	bd3aaf7b-4067-37fb-b207-363cd00afc17	2023-08-01 00:00:05.99	rhmnkd
GB-ENG-222	JADOO	2	11.728999999999999	13.0810	5.8220	129.6400	148.5430	DEFAULT	1	-1	113
    */
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

int foo(int b) { return b; }

int main() {
    auto tasks = read_tsv<Task>("3816f181-7751-4146-ae5e-43a7afdd9a37-0.tsv");

    std::cout << "read " << tasks.size() << " tasks" << std::endl;
    std::cout << "total size " << tasks.size() * sizeof(Task) / 1024 / 1024 << " MB.\n";
    std::cout << tasks[0].v;

    auto tagged_tasks = retag(tasks, [](size_t, const Task& t) { return t.associate_id; });

    std::cout << "First and last tasks after tagging with associate_id:\n";
    std::cout << tagged_tasks[0].v;
    std::cout << tagged_tasks[tagged_tasks.size() - 1].v;

    auto task_NAET = Apply::value(tagged_tasks, [](const Task& t) { return t.net_associate_effort; });
    auto NAET_moments = Reduce::moments(task_NAET);
    auto NAET = materialize(Apply::value(NAET_moments, [](const auto& m) { return m.mean(); }));

    std::cout << NAET;
}