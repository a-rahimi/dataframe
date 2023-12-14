#include <algorithm>
#include <iostream>
#include <memory>
#include <utility>
#include <vector>

#include "expressions.h"

struct RangeTag {};

struct NoTag {};

struct NoValue {};

template <typename Tag, typename Value>
struct IndexMergeOp {
    template <typename ValueOther>
    auto operator()(Tag tag, Tag, Value v, ValueOther) const {
        return std::pair(tag, v);
    }
};

template <typename _Tag, typename _Value>
struct DataFrame {
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
        return Expr_Intersection(to_expr(*this), to_expr(index), IndexMergeOp<Tag, Value>());
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

template <typename Tag, typename Value>
std::ostream &operator<<(std::ostream &s, const DataFrame<Tag, Value> &df) {
    for (size_t i = 0; i < df.size(); ++i)
        s << df.tags[i] << '\t' << df.values[i] << std::endl;
    return s;
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

    DataFrame(const std::vector<RangeTag> &_tags, const std::vector<Value> &_values)
        : tags(new auto(_tags)), values(new auto(_values)) {}

    size_t size() const { return values->size(); }

    template <typename ValueOther>
    auto operator[](const DataFrame<size_t, ValueOther> &index) {
        return Expr_Intersection(to_expr(*this), to_expr(index), IndexMergeOp<size_t, Value>());
    }
};

// DataFrames of type NoValue use a special version of std::vector that takes up
// no space.
template <>
struct std::vector<NoValue> {
    auto operator[](size_t i) const { return NoValue(); }
};

template <typename Tag>
std::ostream &operator<<(std::ostream &s, const DataFrame<Tag, NoValue> &df) {
    s << '[';
    for (size_t i = 0; i < df.size(); ++i)
        s << df.tags[i] << ", ";
    s << ']';
    return s;
}

namespace Reduce {
template <typename ReduceOp>
struct ReduceAdaptor {
    ReduceOp op;

    ReduceAdaptor(ReduceOp _op) : op(_op) {}

    template <typename Tag, typename Value>
    auto operator()(Tag t, Value v) {
        using ValueOutput = decltype(op(v, v));
        return std::pair(t, ValueOutput(v));
    }

    template <typename Tag, typename Value, typename ValueAccumulator>
    auto operator()(Tag t1, Tag, Value v1, ValueAccumulator v2) {
        return std::pair(t1, op(v1, v2));
    }
};

template <typename Expr, typename ReduceOp>
static auto reduce(Expr df, ReduceOp op) {
    return Expr_Reduction(to_expr(df), ReduceAdaptor(op));
}

template <typename Expr>
static auto sum(Expr df) {
    return reduce(df, std::plus<>());
}

template <typename Expr>
static auto max(Expr df) {
    return reduce(df, [](typename Expr::Value v1, typename Expr::Value v2) { return v1 > v2 ? v1 : v2; });
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