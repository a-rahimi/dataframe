#include <algorithm>
#include <initializer_list>
#include <iostream>
#include <type_traits>
#include <utility>
#include <vector>

// Forward declaration of a materialized dataframe.
template <typename Tag, typename Value>
struct DataFrame;

// A wrapper for a materialized dataframe.
template <typename _Tag, typename _Value>
struct Expr_DataFrame {
    using Tag = _Tag;
    using Value = _Value;

    DataFrame<Tag, Value> df;
    size_t i;
    Tag tag;
    Value value;

    Expr_DataFrame(DataFrame<Tag, Value> _df) : df(_df), i(0) { update_tagvalue(); }

    void update_tagvalue() {
        tag = df.tags[i];
        value = df.values[i];
    }

    bool end() const { return i >= df.size(); }

    void next() {
        ++i;
        update_tagvalue();
    }

    void advance_to_tag(Tag t) {
        i = std::lower_bound(df.tags.begin(), df.tags.end(), t) - df.tags.begin();
        update_tagvalue();
    }
};

// A wrapper for a materialized RangeTag dataframe.
struct RangeTag {};

template <typename _Value>
struct Expr_DataFrame<RangeTag, _Value> {
    using Tag = size_t;
    using Value = _Value;

    DataFrame<RangeTag, Value> df;
    size_t i;
    Tag tag;
    Value value;

    Expr_DataFrame(DataFrame<RangeTag, Value> _df) : df(_df) { update_tagvalue(0); }

    void update_tagvalue(size_t _i) {
        i = _i;
        tag = _i;
        value = df.values[_i];
    }

    bool end() const { return i >= df.size(); }

    void next() { update_tagvalue(i + 1); }

    void advance_to_tag(Tag t) { update_tagvalue(t); }
};

// Convert a dataframe to a Expr_DataFrame. If the argument is already a Expr_DataFrame, just return it as is.
template <typename Tag, typename Value>
auto to_expr(DataFrame<Tag, Value> df) {
    return Expr_DataFrame(df);
}

template <typename Expr>
auto to_expr(Expr df) {
    return df;
}

template <typename Expr, typename Tag>
void advance_to_tag_by_linear_search(Expr &df, Tag t) {
    while (!df.end() && (df.tag != t))
        df.next();
}

// Reduces entries of a dataframe that have the same tag.
template <typename Expr, typename ReduceOp>
struct Expr_Reduction {
    Expr df;
    ReduceOp reduce_op;

    using Tag = decltype(reduce_op(df.tag, df.tag, df.value, df.value).first);
    using Value = decltype(reduce_op(df.tag, df.tag, df.value, df.value).second);

    Tag tag;
    Value value;
    bool _end;

    Expr_Reduction(Expr _df, ReduceOp _reduce_op) : df(_df), reduce_op(_reduce_op), _end(false) { next(); }

    void next() {
        _end = df.end();

        Tag current_tag = df.tag;
        auto accumulation = reduce_op(df.tag, df.value);

        for (df.next(); !df.end() && (df.tag == current_tag); df.next())
            accumulation = reduce_op(df.tag, accumulation.first, df.value, accumulation.second);

        tag = accumulation.first;
        value = accumulation.second;
    }

    bool end() const { return _end; }

    void advance_to_tag(Tag t) { advance_to_tag_by_linear_search(*this, t); }
};

// Zip two dataframes.
template <typename Expr1, typename Expr2, typename MergeOp>
struct Expr_Intersection {
    Expr1 df1;
    Expr2 df2;
    MergeOp merge_op;

    using Tag = decltype(merge_op(df1.tag, df2.tag, df1.value, df2.value).first);
    using Value = decltype(merge_op(df1.tag, df2.tag, df1.value, df2.value).second);

    Tag tag;
    Value value;
    bool _end;

    Expr_Intersection(Expr1 _df1, Expr2 _df2, MergeOp _merge_op)
        : df1(_df1), df2(_df2), merge_op(_merge_op), _end(false) {
        next();
    }

    void next() {
        _end = df2.end();
        while (!df2.end()) {
            df1.advance_to_tag(df2.tag);
            if (df1.end())
                continue;

            auto tagvalue = merge_op(df1.tag, df2.tag, df1.value, df2.value);
            tag = tagvalue.first;
            value = tagvalue.second;
            df2.next();
            break;
        }
    }

    bool end() const { return _end; }

    void advance_to_tag(Tag t) { advance_to_tag_by_linear_search(*this, t); }
};

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
    std::vector<Tag> tags;
    std::vector<Value> values;

    size_t size() const { return tags.size(); }

    template <typename ValueOther>
    auto operator[](const DataFrame<Tag, ValueOther> &index) {
        return Expr_Intersection(to_expr(*this), to_expr(index), IndexMergeOp<Tag, Value>());
    }
};

template <typename Expr>
auto materialize(Expr edf) {
    DataFrame<decltype(edf.tag), decltype(edf.value)> mdf;
    for (; !edf.end(); edf.next()) {
        mdf.tags.push_back(edf.tag);
        mdf.values.push_back(edf.value);
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

    std::vector<RangeTag> tags;
    std::vector<Value> values;

    size_t size() const { return values.size(); }

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
template <typename Value, typename ReduceOp>
struct ReduceAdaptor {
    Value init;
    ReduceOp op;

    ReduceAdaptor(Value _init, ReduceOp _op) : init(_init), op(_op) {}

    template <typename Tag>
    auto operator()(Tag t, Value v) {
        return (*this)(t, t, v, init);
    }

    template <typename Tag, typename ValueAccumulator>
    auto operator()(Tag t1, Tag, Value v1, ValueAccumulator v2) {
        return std::pair(t1, op(v1, v2));
    }
};

template <typename Expr>
static auto sum(Expr df) {
    return Expr_Reduction(to_expr(df), ReduceAdaptor(typename Expr::Value(0), std::plus<>()));
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
