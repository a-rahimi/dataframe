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

template <typename Expr>
auto materialize(Expr edf) {
    DataFrame<typename Expr::Tag, typename Expr::Value> mdf;
    for (; !edf.end(); edf.next()) {
        mdf.tags->push_back(edf.tag);
        mdf.values->push_back(edf.value);
    }
    return mdf;
}

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
struct DataFrame<RangeTag, _Value> {
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

template <typename Tag, typename Value>
auto sort_tags(DataFrame<Tag, Value> df) {
    // A sequence of numbers from 0...|df|
    size_t indices[df.size()];
    std::iota(indices, indices + df.size(), 0);

    // Sort the sequence but use the dataframe's tags to compare.
    const auto &tags = *df.tags;
    const auto &values = *df.values;
    std::sort(indices, indices + df.size(), [tags](size_t a, size_t b) { return tags[a] < tags[b]; });

    // Create a new dataframe from scratch with the entries reordered.
    DataFrame<Tag, Value> dfnew;
    dfnew.tags->reserve(df.size());
    dfnew.values->reserve(df.size());

    for (size_t i = 0; i < df.size(); ++i) {
        dfnew.tags->push_back(tags[indices[i]]);
        dfnew.values->push_back(values[indices[i]]);
    }
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
    for (size_t i = 0; i < df.size(); ++i)
        dfo.tags->push_back(compute_tag((*df.tags)[i], (*df.values)[i]));

    return sort_tags(dfo);
}

// DataFrames of type NoValue use a special version of std::vector that takes up
// no space.
template <>
struct std::vector<NoValue> {
    auto operator[](size_t i) const { return NoValue(); }
};

namespace Reduce {
template <typename ReduceOp, typename InitOp>
struct ReduceAdaptor {
    ReduceOp op;
    InitOp initop;

    ReduceAdaptor(ReduceOp _op, InitOp _initop) : op(_op), initop(_initop) {}

    template <typename Tag, typename Value>
    auto operator()(Tag, Value v1) {
        return initop(v1);
    }

    template <typename Tag, typename Value, typename ValueAccumulator>
    auto operator()(Tag, Value v1, ValueAccumulator v2) {
        return op(v1, v2);
    }
};

template <typename Expr, typename ReduceOp, typename ValueAccumulator>
static auto reduce(Expr df, ReduceOp op, ValueAccumulator init) {
    return Expr_Reduction(to_expr(df), ReduceAdaptor(op, init));
}

template <typename Expr>
auto moments(Expr df) {
    struct Moments {
        size_t count;
        Expr::Value sum;
        Expr::Value sum_squares;

        auto mean() const { return sum / count; }
        auto var() const { return sum_squares / count - mean() * mean(); }
        auto std() const { return std::sqrt(var()); }

        Moments operator()(Expr::Tag, Expr::Value v) { return Moments{1, v, v * v}; }

        Moments operator()(Expr::Tag, Expr::Value v, const Moments &m) {
            return Moments{m.count + 1, m.sum + v, m.sum_squares + v * v};
        }
    };

    return Expr_Reduction(to_expr(df), Moments());
}

template <typename Expr>
static auto count(Expr df) {
    return reduce(
        df, [](const Expr::Value &, size_t acc) { return acc + 1; }, [](const Expr::Value &) { return size_t(1); });
}

template <typename Expr>
static auto sum(Expr df) {
    return reduce(df, std::plus<>(), std::identity());
}

template <typename Expr>
static auto max(Expr df) {
    return reduce(
        df, [](typename Expr::Value v1, typename Expr::Value v2) { return v1 > v2 ? v1 : v2; }, std::identity());
}

};  // namespace Reduce

namespace Join {
template <typename CollateOp>
struct CollateAdaptor {
    CollateOp op;

    CollateAdaptor(CollateOp _op) : op(_op) {}

    template <typename Tag1, typename Tag2, typename Value1, typename Value2>
    auto operator()(Tag1 t1, Tag2, Value1 v1, Value2 v2) {
        return std::pair(t1, op(v1, v2));
    }
};

template <typename Expr1, typename Expr2, typename CollateOp>
static auto collate(Expr1 df1, Expr2 df2, CollateOp op) {
    return Expr_Intersection(to_expr(df1), to_expr(df2), CollateAdaptor(op));
}

template <typename Expr1, typename Expr2>
static auto sum(Expr1 df1, Expr2 df2) {
    return Expr_Intersection(to_expr(df1), to_expr(df2), CollateAdaptor(std::plus<>()));
}
};  // namespace Join

template <typename Expr1, typename Expr2>
auto concatenate(Expr1 df1, Expr2 df2) {
    return Expr_Union(to_expr(df1), to_expr(df2));
}