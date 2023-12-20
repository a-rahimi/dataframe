#include <algorithm>
#include <cmath>
#include <functional>
#include <memory>
#include <numeric>
#include <utility>
#include <vector>

#include "expressions.h"
#include "formatting.h"

struct RangeTag {};

struct NoTag {};

struct NoValue {};

/* A materialized dataframe.

Dataframes are light-weight to copy because they're copied by reference.
Dataframes contain at most two reference-counted pointers, one to a tags array
(unless the tags are RangeTags, in which the tags array is not explicitly
stored) , and one to a values array (unless the value type is NoValue). Copy a
dataframe, or passing it by reference copies these arrays. Notably, modifying a
copy of a dataframe modifies the tags and values of the original.
*/
template <typename _Tag, typename _Value>
struct DataFrame : Operations<DataFrame<_Tag, _Value>> {
    using Tag = _Tag;
    using Value = _Value;

    std::shared_ptr<std::vector<Tag>> tags;
    std::shared_ptr<std::vector<Value>> values;

    DataFrame() : tags(new std::vector<Tag>), values(new std::vector<Value>) {}
    DataFrame(const std::vector<Tag> &_tags, const std::vector<Value> &_values)
        : tags(new auto(_tags)), values(new auto(_values)) {}

    size_t size() const { return tags->size(); }

    template <typename ValueOther>
    auto operator[](const DataFrame<Tag, ValueOther> &index) {
        return Expr_Intersection(
            to_expr(*this), to_expr(index), [](Tag tag, Tag, Value v, ValueOther) { return std::pair(tag, v); });
    }

    auto operator[](size_t i) {
        struct TagValue {
            const Tag &t;
            Value &v;
        };
        return TagValue{(*tags)[i], (*values)[i]};
    }
    auto operator[](size_t i) const {
        struct TagValueConst {
            const Tag &t;
            const Value &v;
        };
        return TagValueConst{(*tags)[i], (*values)[i]};
    }
};

// A std::vector for sequential tags. Instead of storing the tag values,
// computes them as needed.
template <>
struct std::vector<RangeTag> {
    size_t sz;

    size_t size() const { return sz; }
    size_t operator[](size_t i) const { return i; }
};

/* Dataframes specialized to range tags.

These require special handling because the indexing operation returns a
different type of dataframe.
*/
template <typename _Value>
struct DataFrame<RangeTag, _Value> : Operations<DataFrame<RangeTag, _Value>> {
    using Tag = size_t;
    using Value = _Value;

    std::shared_ptr<std::vector<RangeTag>> tags;
    std::shared_ptr<std::vector<Value>> values;

    DataFrame() : tags(new std::vector<RangeTag>), values(new std::vector<Value>) {}

    DataFrame(const std::vector<Value> &_values)
        : tags(new std::vector<RangeTag>{_values.size()}), values(new auto(_values)) {}

    DataFrame(const std::vector<RangeTag> &_tags, const std::vector<Value> &_values)
        : tags(new auto(_tags)), values(new auto(_values)) {}

    size_t size() const { return values->size(); }

    template <typename ValueOther>
    auto operator[](const DataFrame<size_t, ValueOther> &index) {
        return Expr_Intersection(
            to_expr(*this), to_expr(index), [](size_t tag, size_t, Value v, ValueOther) { return std::pair(tag, v); });
    }

    auto operator[](size_t i) {
        struct TagValue {
            Tag t;
            Value &v;
        };
        return TagValue{i, (*values)[i]};
    }
    auto operator[](size_t i) const {
        struct TagValueConst {
            const Tag t;
            const Value &v;
        };
        return TagValueConst{i, (*values)[i]};
    }
};
static struct Timer {
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
} timer;

template <typename Tag, typename Value>
auto sort_tags(DataFrame<Tag, Value> df) {
    // A sequence of numbers from 0...|df|
    size_t indices[df.size()];
    std::iota(indices, indices + df.size(), 0);

    // Sort the sequence but use the dataframe's tags to compare.
    const auto &tags = *df.tags;
    const auto &values = *df.values;
    timer.start("Sort");
    std::sort(indices, indices + df.size(), [tags](size_t a, size_t b) { return tags[a] < tags[b]; });
    timer.stop();

    // Create a new dataframe from scratch with the entries reordered.
    DataFrame<Tag, Value> dfnew;

    timer.start("Reorder");
    dfnew.tags->reserve(df.size());
    dfnew.values->reserve(df.size());

    for (size_t i = 0; i < df.size(); ++i) {
        dfnew.tags->push_back(tags[indices[i]]);
        dfnew.values->push_back(values[indices[i]]);
    }
    timer.stop();
    return dfnew;
}

template <typename Tag, typename Value, typename TagOp>
auto retag(DataFrame<Tag, Value> df, TagOp compute_tag) {
    // Create an emtpy dataframe.
    using TagOut = decltype(compute_tag((*df.tags)[0], (*df.values)[0]));
    DataFrame<TagOut, Value> dfo;

    // shallow-copy the old values.
    dfo.values = df.values;

    // create a new tags array from scratch with the computed tags.
    timer.start("Compute new tags");
    for (size_t i = 0; i < df.size(); ++i)
        dfo.tags->push_back(compute_tag((*df.tags)[i], (*df.values)[i]));
    timer.stop();

    return sort_tags(dfo);
}

// DataFrames of type NoValue use a special version of std::vector that takes up
// no space.
template <>
struct std::vector<NoValue> {
    auto operator[](size_t i) const { return NoValue(); }
};