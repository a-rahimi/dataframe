#include <concepts>
#include <map>
#include <type_traits>

// Forward declaration of a materialized dataframe.
template <typename Tag, typename Value>
struct DataFrame;
template <typename Derived>
struct Operations;

// A version of std::reference_wrapper that's equipped with a default constructor.
template <typename T>
struct reference {
    T const *ptr;

    reference() : ptr(0) {}
    reference &operator=(const T &v) {
        ptr = &v;
        return *this;
    }
    operator const T &() const { return *ptr; }
    bool operator==(reference<T> other) { return *ptr == *other.ptr; }
};

// A wrapper for a materialized dataframe.
template <typename _Tag, typename _Value>
struct Expr_DataFrame : Operations<Expr_DataFrame<_Tag, _Value>> {
    using Tag = DataFrame<_Tag, _Value>::Tag;
    using Value = DataFrame<_Tag, _Value>::Value;

    DataFrame<_Tag, _Value> df;
    size_t i;
    reference<Tag> tag;
    reference<Value> value;

    Expr_DataFrame(DataFrame<_Tag, _Value> _df) : df(_df) { update_tagvalue(0); }

    void update_tagvalue(size_t _i) {
        i = _i;
        tag = (*df.tags)[i];
        value = (*df.values)[i];
    }

    bool end() const { return i >= df.size(); }

    void next() { update_tagvalue(i + 1); }

    void advance_to_tag(Tag t) {
        auto l = std::lower_bound(df.tags->begin(), df.tags->end(), t);
        if (*l == t)
            update_tagvalue(l - df.tags->begin());
        else
            i = df.size();  // Didn't find the tag. It's the end of this expression.
    }
};

struct RangeTag;

// A wrapper for a materialized RangeTag dataframe.
template <typename _Value>
struct Expr_DataFrame<RangeTag, _Value> : Operations<Expr_DataFrame<RangeTag, _Value>> {
    using Tag = size_t;
    using Value = DataFrame<RangeTag, _Value>::Value;

    DataFrame<RangeTag, Value> df;
    size_t i;
    Tag tag;
    reference<Value> value;

    Expr_DataFrame(DataFrame<RangeTag, _Value> _df) : df(_df) { update_tagvalue(0); }

    void update_tagvalue(size_t _i) {
        i = _i;
        if (_i >= df.size())
            return;

        tag = _i;
        value = (*df.values)[_i];
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
auto to_expr(Expr &df) {
    return df;
}

// Materialize an expression to a dataframe, and leave a dataframe intact.
template <typename Tag, typename Value>
auto to_dataframe(DataFrame<Tag, Value> df) {
    return df;
}

template <typename Expr>
auto to_dataframe(Expr df) {
    return df.materialize();
}

template <typename Expr, typename Tag>
void advance_to_tag_by_linear_search(Expr &df, Tag t) {
    while (!df.end() && (df.tag != t))
        df.next();
}

// Compute the ordering of the elemnts of a vector that would cause it to get
// sorted.
template <typename T>
void argsort(const std::vector<T> &array, std::vector<size_t> &indices) {
    std::map<T, std::vector<size_t>> indices_map;

    for (size_t i = 0; i < array.size(); ++i)
        indices_map[array[i]].push_back(i);

    // Read the map back in sorted order and store the indices.
    for (const auto &[tag, indices_for_tag] : indices_map)
        for (size_t i : indices_for_tag)
            indices.push_back(i);
}

// Compute new tags on a materialized dataframe.
template <typename TagT, typename ValueT, typename TagV, typename ValueV>
struct Expr_Retag : Operations<Expr_Retag<TagT, ValueT, TagV, ValueV>> {
    DataFrame<TagT, ValueT> df_tags;
    DataFrame<TagV, ValueV> df_values;

    using Tag = DataFrame<TagT, ValueT>::Value;
    using Value = DataFrame<TagV, ValueV>::Value;

    reference<Tag> tag;
    reference<Value> value;
    size_t i;
    std::shared_ptr<std::vector<size_t>> traversal_order;

    Expr_Retag(DataFrame<TagT, ValueT> _df_tags, DataFrame<TagV, ValueV> _df_values)
        : df_tags(_df_tags), df_values(_df_values), traversal_order(new std::vector<size_t>) {
        if (df_tags.size() != df_values.size())
            throw std::invalid_argument("df_tags and df_values must have the same length");
        argsort(*df_tags.values, *traversal_order);
        update_tagvalue(0);
    }

    void update_tagvalue(size_t _i) {
        i = _i;
        if (end())
            return;
        tag = (*df_tags.values)[(*traversal_order)[_i]];
        value = (*df_values.values)[(*traversal_order)[_i]];
    }

    void next() { update_tagvalue(i + 1); }

    bool end() const { return i >= df_values.size(); }

    // TODO: There's a faster way to advance to a tag. We just need to advance to the
    // tag in df and map to the index of that tag.
    void advance_to_tag(Tag t) { advance_to_tag_by_linear_search(*this, t); }
};

// Reduces entries of a dataframe that have the same tag.
template <typename Expr, typename ReduceOp>
struct Expr_Reduction : Operations<Expr_Reduction<Expr, ReduceOp>> {
    Expr df;
    ReduceOp reduce_op;

    using Tag = typename Expr::Tag;
    using Value = std::invoke_result_t<ReduceOp, typename Expr::Tag, typename Expr::Value>;

    reference<Tag> tag;
    Value value;
    bool _end;

    Expr_Reduction(Expr _df, ReduceOp _reduce_op) : df(_df), reduce_op(_reduce_op), _end(false) { next(); }

    void next() {
        _end = df.end();

        tag = df.tag;
        auto accumulation = reduce_op(df.tag, df.value);

        for (df.next(); !df.end() && (df.tag == tag); df.next())
            accumulation = reduce_op(df.tag, df.value, accumulation);

        value = accumulation;
    }

    bool end() const { return _end; }

    void advance_to_tag(Tag t) { advance_to_tag_by_linear_search(*this, t); }
};

// Applies a function to every entry of a dataframe.
template <typename Expr, std::invocable<typename Expr::Tag, typename Expr::Value> Op>
struct Expr_Apply : Operations<Expr_Apply<Expr, Op>> {
    Expr df;
    Op op;

    using Tag = typename Expr::Tag;
    using Value = std::invoke_result_t<Op, typename Expr::Tag, typename Expr::Value>;

    reference<Tag> tag;
    Value value;
    bool _end;

    Expr_Apply(Expr _df, Op _op) : df(_df), op(_op), _end(false) { next(); }

    void next() {
        _end = df.end();

        tag = df.tag;
        value = op(df.tag, df.value);

        df.next();
    }

    bool end() const { return _end; }

    void advance_to_tag(Tag t) {
        df.advance_to_tag(t);
        next();
    }
};

// Inner join two dataframes.
template <typename Expr1, typename Expr2,
          std::invocable<typename Expr1::Tag, typename Expr1::Value, typename Expr2::Value> MergeOp>
struct Expr_Intersection : Operations<Expr_Intersection<Expr1, Expr2, MergeOp>> {
    Expr1 df1;
    Expr2 df2;
    MergeOp merge_op;

    using Tag = typename Expr2::Tag;
    using Value = std::invoke_result_t<MergeOp, Tag, typename Expr1::Value, typename Expr2::Value>;

    Tag tag;
    Value value;
    bool _end;

    Expr_Intersection(Expr1 _df1, Expr2 _df2, MergeOp _merge_op)
        : df1(_df1), df2(_df2), merge_op(_merge_op), _end(false) {
        next();
    }

    void next() {
        _end = df2.end();
        for (; !df2.end(); df2.next()) {
            df1.advance_to_tag(df2.tag);
            if (df1.end())
                continue;  // df1 has no matching tag. Move to the next tag in df2.

            tag = df2.tag;
            value = merge_op(df1.tag, df1.value, df2.value);

            df2.next();
            break;
        }
    }

    bool end() const { return _end; }

    void advance_to_tag(Tag t) { advance_to_tag_by_linear_search(*this, t); }
};

// Outer join two dataframes.
template <typename Expr1, typename Expr2>
struct Expr_Union : Operations<Expr_Union<Expr1, Expr2>> {
    Expr1 df1;
    Expr2 df2;

    using Tag = Expr1::Tag;
    using Value = Expr1::Value;

    reference<Tag> tag;
    reference<Value> value;
    bool _end;

    Expr_Union(Expr1 _df1, Expr2 _df2) : df1(_df1), df2(_df2), _end(false) { next(); }

    void next() {
        _end = df1.end() && df2.end();

        if (!df1.end() && ((df1.tag < df2.tag) || df2.end())) {
            tag = df1.tag;
            value = df1.value;
            df1.next();
            return;
        }

        if (!df2.end() && ((df2.tag <= df1.tag) || df1.end())) {
            tag = df2.tag;
            value = df2.value;
            df2.next();
        }
    }

    bool end() const { return _end; }

    void advance_to_tag(Tag t) { advance_to_tag_by_linear_search(*this, t); }
};

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

template <typename Tag, typename Value>
struct Moments {
    size_t count;
    Value sum;
    Value sum_squares;

    auto mean() const { return sum / count; }
    auto var() const { return sum_squares / count - mean() * mean(); }
    auto std() const { return std::sqrt(var()); }

    Moments operator()(Tag, const Value &v) { return Moments{1, v, v * v}; }

    Moments operator()(Tag, const Value &v, const Moments &m) {
        return Moments{m.count + 1, m.sum + v, m.sum_squares + v * v};
    }
};

// The base class for Expr_*'s and materialized dataframes. These operations can
// be applied to both.
template <typename Derived>
struct Operations {
    auto to_expr() { return ::to_expr(static_cast<Derived &>(*this)); }

    auto to_dataframe() { return ::to_dataframe(static_cast<Derived &>(*this)); }

    template <std::invocable<typename Derived::Tag, typename Derived::Value> RetagOp>
    auto retag(RetagOp compute_tag) {
        auto df = static_cast<Derived &>(*this);
        auto df_tags = df.apply(compute_tag).materialize();
        return Expr_Retag(df_tags, df);
    }

    template <typename Expr>
    auto retag(Expr tag_expr) {
        auto df = static_cast<Derived &>(*this);
        return Expr_Retag(tag_expr.to_dataframe(), df);
    }

    template <std::invocable<typename Derived::Tag, typename Derived::Value> ApplyOp>
    auto apply(ApplyOp op) {
        return Expr_Apply(to_expr(), op);
    }

    template <std::invocable<typename Derived::Value> ApplyOp>
    auto apply(ApplyOp op) {
        return Expr_Apply(to_expr(), [&op](typename Derived::Tag, const typename Derived::Value &v) { return op(v); });
    }

    template <typename ReduceOp, typename ValueAccumulator>
    auto reduce(ReduceOp op, ValueAccumulator init) {
        return Expr_Reduction(to_expr(), ReduceAdaptor(op, init));
    }

    auto reduce_moments() {
        return Expr_Reduction(to_expr(), Moments<typename Derived::Tag, typename Derived::Value>());
    }

    auto reduce_mean() {
        return reduce_moments().apply([](const auto &m) { return m.mean(); });
    }

    auto reduce_var() {
        return reduce_moments().apply([](const auto &m) { return m.var(); });
    }

    auto reduce_std() {
        return reduce_moments().apply([](const auto &m) { return m.std(); });
    }

    auto reduce_count() {
        // We could use reduce_moments() to implement this function, but I decided to exercise the
        // reduce() function for now. Once reduce() has had enough exercise, it might make sense
        // to implement this the same way reduce_mean is implemented.
        return reduce([](const Derived::Value &, size_t acc) { return acc + 1; },
                      [](const Derived::Value &) { return size_t(1); });
    }

    auto reduce_sum() { return reduce(std::plus<>(), std::identity()); }

    auto reduce_max() {
        return reduce([](Derived::Value v1, Derived::Value v2) { return v1 > v2 ? v1 : v2; }, std::identity());
    }

    template <typename Expr, std::invocable<typename Derived::Value, typename Expr::Value> CollateOp>
    auto collate(Expr df_other, CollateOp op) {
        return Expr_Intersection(
            to_expr(),
            df_other.to_expr(),
            [&op](const typename Expr::Tag &, const typename Derived::Value &v1, const typename Expr::Value &v2) {
                return op(v1, v2);
            });
    }

    template <typename Expr>
    auto concatenate(Expr df_other) {
        return Expr_Union(to_expr(), df_other.to_expr());
    }

    auto materialize() {
        auto expr = static_cast<Derived &>(*this);

        DataFrame<typename Derived::Tag, typename Derived::Value> mdf;
        for (; !expr.end(); expr.next()) {
            mdf.tags->push_back(expr.tag);
            mdf.values->push_back(expr.value);
        }
        return mdf;
    }

    auto operator*() { return materialize(); }
};
