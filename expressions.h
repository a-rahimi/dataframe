#include <algorithm>

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

// Inner join two dataframes.
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

// Outer join two dataframes.
template <typename Expr1, typename Expr2>
struct Expr_Union {
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
