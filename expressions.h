#include <type_traits>

// Forward declaration of a materialized dataframe.
template <typename Tag, typename Value>
struct DataFrame;
template <typename Derived>
struct Operations;

/* A version of std::reference_wrapper that's equipped with a default constructor.
 */
template <typename T>
struct reference {
    T *ptr;

    reference() : ptr(0) {}
    reference &operator=(T &&v) {
        ptr = &v;
        return *this;
    }
    reference &operator=(T &v) {
        ptr = &v;
        return *this;
    }
    operator T &() const { return *ptr; }
    bool operator==(reference<T> other) { return *ptr == *other.ptr; }
};

// A wrapper for a materialized dataframe.
template <typename _Tag, typename _Value>
struct Expr_DataFrame : Operations<Expr_DataFrame<_Tag, _Value>> {
    using Tag = _Tag;
    using Value = _Value;

    DataFrame<Tag, Value> df;
    size_t i;
    reference<Tag> tag;
    reference<Value> value;

    Expr_DataFrame(DataFrame<Tag, Value> _df) : df(_df), i(0) { update_tagvalue(); }

    void update_tagvalue() {
        tag = (*df.tags)[i];
        value = (*df.values)[i];
    }

    bool end() const { return i >= df.size(); }

    void next() {
        ++i;
        update_tagvalue();
    }

    void advance_to_tag(Tag t) {
        i = std::lower_bound(df.tags->begin(), df.tags->end(), t) - df.tags->begin();
        update_tagvalue();
    }
};

struct RangeTag;

// A wrapper for a materialized RangeTag dataframe.
template <typename _Value>
struct Expr_DataFrame<RangeTag, _Value> : Operations<Expr_DataFrame<RangeTag, _Value>> {
    using Tag = size_t;
    using Value = _Value;

    DataFrame<RangeTag, Value> df;
    size_t i;
    Tag tag;
    reference<Value> value;

    Expr_DataFrame(DataFrame<RangeTag, Value> _df) : df(_df) { update_tagvalue(0); }

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
auto to_expr(Expr df) {
    return df;
}

template <typename Expr, typename Tag>
void advance_to_tag_by_linear_search(Expr &df, Tag t) {
    while (!df.end() && (df.tag != t))
        df.next();
}

// Compute new tags on a materialized dataframe.
template <typename Tag1, typename Value1, typename Tag2, typename Value2>
struct Expr_Retag : Operations<Expr_Retag<Tag1, Value1, Tag2, Value2>> {
    DataFrame<Tag1, Value1> df_tags;
    DataFrame<Tag2, Value2> df_values;

    using Tag = Value1;
    using Value = Value2;

    reference<Tag> tag;
    reference<Value> value;
    size_t i;
    std::shared_ptr<std::vector<size_t>> traversal_order;

    Expr_Retag(DataFrame<Tag1, Tag> _df_tags, DataFrame<Tag2, Value> _df_values)
        : df_tags(_df_tags), df_values(_df_values), traversal_order(new std::vector<size_t>(_df_values.size())) {
        if (df_tags.size() != df_values.size())
            throw std::invalid_argument("df_tags and df_values must have the same length");

        // compute the ordering.
        std::iota(traversal_order->begin(), traversal_order->end(), 0);
        const auto &tags = *df_tags.values;
        std::sort(
            traversal_order->begin(), traversal_order->end(), [tags](size_t a, size_t b) { return tags[a] < tags[b]; });

        update_tagvalue(0);
    }

    void update_tagvalue(size_t _i) {
        i = _i;
        if (_i >= df_values.size())
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
template <typename Expr, typename Op>
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
template <typename Expr1, typename Expr2, typename MergeOp>
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
        while (!df2.end()) {
            df1.advance_to_tag(df2.tag);
            if (df1.end())
                continue;

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

template <typename Op>
struct EmptyTagAdaptor {
    Op op;
    EmptyTagAdaptor(Op _op) : op(_op) {}
    template <typename Tag, typename Value>
    auto operator()(const Tag &t, const Value &v) {
        return op(v);
    }
};

template <typename CollateOp>
struct WithTagPlaceholder {
    CollateOp op;

    WithTagPlaceholder(CollateOp _op) : op(_op) {}

    template <typename Tag, typename Value1, typename Value2>
    auto operator()(Tag, Value1 v1, Value2 v2) {
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

    Moments operator()(Tag, Value v) { return Moments{1, v, v * v}; }

    Moments operator()(Tag, Value v, const Moments &m) {
        return Moments{m.count + 1, m.sum + v, m.sum_squares + v * v};
    }
};

template <typename Derived>
struct Operations {
    auto to_expr() { return ::to_expr(static_cast<Derived &>(*this)); }

    template <typename RetagOp>
    auto retag(RetagOp compute_tag) {
        auto df = static_cast<Derived &>(*this);
        auto df_tags = df.apply_to_tags_and_values(compute_tag).materialize();
        return Expr_Retag(df_tags, df);
    }

    template <typename ApplyOp>
    auto apply_to_tags_and_values(ApplyOp op) {
        return Expr_Apply(to_expr(), op);
    }

    template <typename ApplyOp>
    auto apply_to_values(ApplyOp op) {
        return Expr_Apply(to_expr(), EmptyTagAdaptor<ApplyOp>(op));
    }

    template <typename ReduceOp, typename ValueAccumulator>
    auto reduce(ReduceOp op, ValueAccumulator init) {
        return Expr_Reduction(to_expr(), ReduceAdaptor(op, init));
    }

    auto reduce_moments() {
        return Expr_Reduction(to_expr(), Moments<typename Derived::Tag, typename Derived::Value>());
    }

    auto mean() {
        return reduce_moments().apply_to_values([](const auto &m) { return m.mean(); });
    }

    auto reduce_count() {
        return reduce([](const Derived::Value &, size_t acc) { return acc + 1; },
                      [](const Derived::Value &) { return size_t(1); });
    }

    auto reduce_sum() { return reduce(std::plus<>(), std::identity()); }

    auto reduce_max() {
        return reduce([](Derived::Value v1, Derived::Value v2) { return v1 > v2 ? v1 : v2; }, std::identity());
    }

    template <typename Expr, typename CollateOp>
    auto collate(Expr df_other, CollateOp op) {
        return Expr_Intersection(to_expr(), df_other.to_expr(), WithTagPlaceholder(op));
    }

    template <typename Expr>
    auto collate_sum(Expr df_other) {
        return Expr_Intersection(to_expr(), df_other.to_expr(), WithTagPlaceholder(std::plus<>()));
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
