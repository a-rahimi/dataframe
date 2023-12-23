#include <cassert>
#include <map>
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
};

std::ostream& operator<<(std::ostream& s, const Task& t) {
    s << t.task_id << ", " << t.inquiry_id << ", " << t.created_time_pt << ", " << t.associate_id << ", " << t.store_id
      << ", " << t.store_format << ", " << t.opportunities << ", " << t.duration_of_task_seconds << ", " << t.ttff
      << ", " << t.buffer_time << t.net_associate_effort << ", " << t.task_latency_s << ", " << t.task_queue << ", "
      << t.quorum_size << ", " << t.num_defects << ", " << t.num_app_events << std::endl;
    return s;
}

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

auto compute_NAET_with_map_optimized(DataFrame<RangeTag, Task> df) {
    struct Accumulator {
        int count;
        float sum;
        Accumulator() : count(0), sum(0) {}
    };
    std::map<std::string, Accumulator> accumulators;

    for (const auto& task : *df.values) {
        auto& acc = accumulators[task.associate_id];
        acc.count++;
        acc.sum += task.net_associate_effort;
    }

    DataFrame<std::string, float> NAET;
    for (const auto& [associate_id, acc] : accumulators) {
        NAET.tags->push_back(associate_id);
        NAET.values->push_back(acc.sum / acc.count);
    }

    return NAET;
}

template <typename KeyOp, typename ValueOp, typename Tag, typename Value>
auto accumulate(DataFrame<Tag, Value> df, KeyOp key_op, ValueOp value_op) {
    using AccumulationKey = std::invoke_result_t<KeyOp, Value>;
    using AccumulationType = std::invoke_result_t<ValueOp, Value>;

    struct Accumulator {
        int count;
        AccumulationType sum;
        Accumulator() : count(0), sum(0) {}
    };

    std::map<AccumulationKey, Accumulator> accumulators;

    for (const auto& v : *df.values) {
        auto& acc = accumulators[key_op(v)];
        acc.count++;
        acc.sum += value_op(v);
    }

    DataFrame<AccumulationKey, float> df_accumulation;
    for (const auto& [key, acc] : accumulators) {
        df_accumulation.tags->push_back(key);
        df_accumulation.values->push_back(acc.sum / acc.count);
    }

    return df_accumulation;
}

auto compute_NAET_with_map_generic(DataFrame<RangeTag, Task> df) {
    return accumulate(
        df, [](const Task& t) { return t.associate_id; }, [](const Task& t) { return t.net_associate_effort; });
}

auto compute_NAET_by_sorting(DataFrame<RangeTag, Task> df) {
    Timer timer;

    timer.start("By sorting: sorting");
    std::vector<size_t> traversal_order(df.size());
    std::iota(traversal_order.begin(), traversal_order.end(), 0);

    const auto& values = *df.values;
    std::sort(traversal_order.begin(), traversal_order.end(), [values](size_t a, size_t b) {
        return values[a].associate_id < values[b].associate_id;
    });
    timer.stop();

    DataFrame<std::string, float> NAET;

    timer.start("By sorting: traversing");
    std::string last_associate_id = df[traversal_order[0]].v.associate_id;
    int count = 0;
    float sum = 0.;
    for (size_t i : traversal_order) {
        const auto& v = (*df.values)[i];
        if (v.associate_id != last_associate_id) {
            assert(count > 0);
            NAET.tags->push_back(last_associate_id);
            NAET.values->push_back(sum / count);

            count = 0;
            sum = 0.;
            last_associate_id = v.associate_id;
        }

        count++;
        sum += v.net_associate_effort;
    }
    NAET.tags->push_back(last_associate_id);
    NAET.values->push_back(sum / count);
    timer.stop();

    return NAET;
}

auto compute_NAET_by_sorting_2(DataFrame<RangeTag, Task> df) {
    Timer timer;
    std::vector<size_t> traversal_order(df.size());
    std::iota(traversal_order.begin(), traversal_order.end(), 0);

    timer.start("By sorting 2: sorting");
    const auto& values = *df.values;
    std::sort(traversal_order.begin(), traversal_order.end(), [values](size_t a, size_t b) {
        return values[a].associate_id < values[b].associate_id;
    });
    timer.stop();

    DataFrame<std::string, float> NAET;

    timer.start("By sorting 2: traversing");
    std::string last_associate_id = df[traversal_order[0]].v.associate_id;
    int count = 0;
    float sum = 0.;
    for (const size_t i : traversal_order) {
        const auto& v = (*df.values)[i];
        if (v.associate_id != last_associate_id) {
            assert(count > 0);
            NAET.tags->push_back(last_associate_id);
            NAET.values->push_back(sum / count);

            count = 0;
            sum = 0.;
            last_associate_id = v.associate_id;
        }

        count++;
        sum += v.net_associate_effort;
    }
    NAET.tags->push_back(last_associate_id);
    NAET.values->push_back(sum / count);
    timer.stop();

    return NAET;
}

auto compute_NAET_by_sorting_3(DataFrame<RangeTag, Task> df) {
    Timer timer;

    struct RecordIndex {
        size_t index_in_df;
        std::string associate_id;
    };

    timer.start("By sorting 3: extracting associate_ids");
    std::vector<RecordIndex> associate_ids;
    for (size_t i = 0; i < df.size(); ++i)
        associate_ids.push_back(RecordIndex{i, (*df.values)[i].associate_id});
    timer.stop();

    timer.start("By sorting 3: sorting");
    std::sort(associate_ids.begin(), associate_ids.end(), [](const RecordIndex& a, const RecordIndex& b) {
        return a.associate_id < b.associate_id;
    });
    timer.stop();

    DataFrame<std::string, float> NAET;

    timer.start("By sorting 3: traversing");
    std::string last_associate_id = df[associate_ids[0].index_in_df].v.associate_id;
    int count = 0;
    float sum = 0.;
    for (const auto& ass : associate_ids) {
        const auto& v = (*df.values)[ass.index_in_df];
        if (v.associate_id != last_associate_id) {
            assert(count > 0);
            NAET.tags->push_back(last_associate_id);
            NAET.values->push_back(sum / count);

            count = 0;
            sum = 0.;
            last_associate_id = v.associate_id;
        }

        count++;
        sum += v.net_associate_effort;
    }
    NAET.tags->push_back(last_associate_id);
    NAET.values->push_back(sum / count);
    timer.stop();

    return NAET;
}

auto compute_NAET_by_sorting_map(DataFrame<RangeTag, Task> df) {
    Timer timer;
    std::map<std::string, std::vector<size_t> > traversal_order;

    timer.start("By sorting map: sorting");
    for (size_t i = 0; i < df.size(); ++i)
        traversal_order[(*df.values)[i].associate_id].push_back(i);
    timer.stop();

    timer.start("By sorting map: traversing");
    DataFrame<std::string, float> NAET;
    for (const auto& [associate_id, indices] : traversal_order) {
        int count = 0;
        float sum = 0.;
        for (size_t i : indices) {
            const auto& v = (*df.values)[i];
            count++;
            sum += v.net_associate_effort;
        }
        NAET.tags->push_back(associate_id);
        NAET.values->push_back(sum / count);
    }
    timer.stop();

    return NAET;
}

int main() {
    Timer timer;

    timer.start("Reading tab-separated file");
    // A materialized dataframe that contains 850k rows.
    auto tasks = read_tsv<Task>("3816f181-7751-4146-ae5e-43a7afdd9a37-0.tsv");
    timer.stop();

    std::cout << "read " << tasks.size() << " tasks" << std::endl;
    std::cout << "total size " << tasks.size() * sizeof(Task) / 1024 / 1024 << " MB.\n";
    std::cout << tasks[0].v;
    std::cout << '\n';

    timer.start("By accumulating with map optimized");
    auto NAET_with_map_optimized = compute_NAET_with_map_optimized(tasks);
    timer.stop();
    std::cout << '\n';

    timer.start("By accumulating with map generic");
    auto NAET_with_map_generic = compute_NAET_with_map_generic(tasks);
    timer.stop();
    std::cout << '\n';

    timer.start("By sorting expr total");
    {
        Timer timer;

        timer.start("By sorting expr: retag expression");
        // An expression (a non-materialized dataframe) where each row is tagged with the associate_id
        // field of the corresponding record.
        auto tasks_retagged = tasks.retag(tasks.apply_to_values([](const Task& t) { return t.associate_id; }));
        timer.stop();

        timer.start("By sorting expr: Computing stats");
        // For each unit associate, compute the average value of the net_associate_effort field,
        // and store the result in a materialized dataframe (the * operator materializes and expression).
        auto NAET = *tasks_retagged.apply_to_values([](const Task& t) { return t.net_associate_effort; }).reduce_mean();
        timer.stop();
    }
    timer.stop();
    std::cout << '\n';

    timer.start("By sorting 2 total");
    auto NAET_by_sorting_2 = compute_NAET_by_sorting_2(tasks);
    timer.stop();
    std::cout << '\n';

    timer.start("By sorting map total");
    auto NAET_by_sorting_map = compute_NAET_by_sorting_map(tasks);
    timer.stop();
    std::cout << '\n';

    timer.start("By sorting 3 total");
    auto NAET_by_sorting_3 = compute_NAET_by_sorting_3(tasks);
    timer.stop();
    std::cout << '\n';

    // std::cout << NAET_by_sorting_map;
}