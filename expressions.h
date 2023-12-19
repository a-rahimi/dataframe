#include <algorithm>

// Forward declaration of a materialized dataframe.
template <typename Tag, typename Value>
struct DataFrame;
template <typename Derived>
struct Operations;

// A wrapper for a materialized dataframe.
template <typename _Tag, typename _Value>
struct Expr_DataFrame : Operations<Expr_DataFrame<_Tag, _Value>> {
    using Tag = _Tag;
    using Value = _Value;

    DataFrame<Tag, Value> df;
    size_t i;
    Tag tag;
    Value value;

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

// A wrapper for a materialized RangeTag dataframe.
struct RangeTag;

template <typename _Value>
struct Expr_DataFrame<RangeTag, _Value> : Operations<Expr_DataFrame<RangeTag, _Value>> {
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

// Reduces entries of a dataframe that have the same tag.
template <typename Expr, typename ReduceOp>
struct Expr_Reduction : Operations<Expr_Reduction<Expr, ReduceOp>> {
    Expr df;
    ReduceOp reduce_op;

    using Tag = typename Expr::Tag;
    using Value = decltype(reduce_op(df.tag, df.value));

    Tag tag;
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
    using Value = decltype(op(df.tag, df.value));

    Tag tag;
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

// Outer join two dataframes.
template <typename Expr1, typename Expr2>
struct Expr_Union : Operations<Expr_Union<Expr1, Expr2>> {
    Expr1 df1;
    Expr2 df2;

    using Tag = Expr1::Tag;
    using Value = Expr1::Value;

    Tag tag;
    Value value;
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
struct CollateAdaptor {
    CollateOp op;

    CollateAdaptor(CollateOp _op) : op(_op) {}

    template <typename Tag1, typename Tag2, typename Value1, typename Value2>
    auto operator()(Tag1 t1, Tag2, Value1 v1, Value2 v2) {
        return std::pair(t1, op(v1, v2));
    }
};

template <typename Derived>
struct Operations {
    auto to_expr() { return ::to_expr(static_cast<Derived &>(*this)); }

    template <typename Op>
    auto apply_to_tags_and_values(Op op) {
        return Expr_Apply(to_expr(), op);
    }

    template <typename Op>
    auto apply_to_values(Op op) {
        return Expr_Apply(to_expr(), EmptyTagAdaptor<Op>(op));
    }

    template <typename ReduceOp, typename ValueAccumulator>
    auto reduce(ReduceOp op, ValueAccumulator init) {
        return Expr_Reduction(to_expr(), ReduceAdaptor(op, init));
    }

    auto reduce_moments() {
        struct Moments {
            size_t count;
            Derived::Value sum;
            Derived::Value sum_squares;

            auto mean() const { return sum / count; }
            auto var() const { return sum_squares / count - mean() * mean(); }
            auto std() const { return std::sqrt(var()); }

            Moments operator()(Derived::Tag, Derived::Value v) { return Moments{1, v, v * v}; }

            Moments operator()(Derived::Tag, Derived::Value v, const Moments &m) {
                return Moments{m.count + 1, m.sum + v, m.sum_squares + v * v};
            }
        };

        return Expr_Reduction(to_expr(), Moments());
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
        return Expr_Intersection(to_expr(), df_other.to_expr(), CollateAdaptor(op));
    }

    template <typename Expr>
    auto collate_sum(Expr df_other) {
        return Expr_Intersection(to_expr(), df_other.to_expr(), CollateAdaptor(std::plus<>()));
    }

    template <typename Expr>
    auto concatenate(Expr df_other) {
        return Expr_Union(to_expr(), df_other.to_expr());
    }
};
