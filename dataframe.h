#include <initializer_list>
#include <iostream>
#include <type_traits>
#include <utility>
#include <vector>

struct NoTag
{
};

struct NoValue
{
};

template <typename Tag, typename Value>
struct IndexReduceOp
{
    // Indexing fails if the tag or values don't have the right type.
    template <typename T, typename TO, typename V, typename VO>
    auto operator()(T tag, TO, V v, VO)
    {
        return std::pair(NoTag(), v);
    }

    // A successful indexing reduction in df1[df2] takes the value of df1, and
    // ignores the value of df2.
    template <typename ValueOther>
    auto operator()(Tag tag, Tag, Value v, ValueOther)
    {
        return std::pair(tag, v);
    }
};

template <typename Tag, typename Value>
struct DataFrame
{
    std::vector<Tag> tags;
    std::vector<Value> values;

    size_t size() const { return tags.size(); }

    template <typename ValueOther>
    DataFrame<Tag, Value> operator[](const DataFrame<Tag, ValueOther> &index)
    {
        return merge(*this, index, IndexReduceOp<Tag, Value>(), IndexReduceOp<Tag, Value>());
    }

    // By default, nothing gets added to the dataframe.
    template <typename T, typename V>
    void push_back(const std::pair<T, V> &)
    {
    }

    // Only push back pairs with the expected tag and type.
    template <>
    void push_back(const std::pair<Tag, Value> &p)
    {
        tags.push_back(p.first);
        values.push_back(p.second);
    }
};

template <typename Tag, typename Value>
std::ostream &
operator<<(std::ostream &s, const DataFrame<Tag, Value> &df)
{
    for (size_t i = 0; i < df.size(); ++i)
        s << df.tags[i] << '\t' << df.values[i] << std::endl;
    return s;
}

struct RangeTag;

// A std::vector for sequential tags. Instead of storing the tag values,
// computes them as needed.
template <>
struct std::vector<RangeTag>
{
    size_t start, end, step = 1;

    size_t size() const { return (end - start) / step; }
    size_t operator[](size_t i) const { return start + i * step; }
};

/* Dataframes specialized to range tags.

These require special handling because the indexing operation returns a
different type of dataframe.
*/
template <typename Value>
struct DataFrame<RangeTag, Value>
{
    std::vector<RangeTag> tags;
    std::vector<Value> values;

    size_t size() const { return values.size(); }

    template <typename ValueOther>
    DataFrame<size_t, Value> operator[](const DataFrame<size_t, ValueOther> &index)
    {
        return merge(*this, index, IndexReduceOp<size_t, Value>(), IndexReduceOp<size_t, Value>());
    }
};

// DataFrames of type NoValue use a special version of std::vector that takes up
// no space.
template <>
struct std::vector<NoValue>
{
    NoValue operator[](size_t i) const { return NoValue(); }
};

template <typename Tag>
std::ostream &
operator<<(std::ostream &s, const DataFrame<Tag, NoValue> &df)
{
    s << '[';
    for (size_t i = 0; i < df.size(); ++i)
        s << df.tags[i] << ", ";
    s << ']';
    return s;
}

template <typename Merge12Op, typename AccumulateOp, typename Tag1, typename Value1, typename Tag2, typename Value2>
auto merge(const DataFrame<Tag1, Value1> &df1, const DataFrame<Tag2, Value2> &df2, Merge12Op merge_12, AccumulateOp accumulate)
{
    using TagOut = decltype(merge_12(df1.tags[0], df2.tags[0], df1.values[0], df2.values[0]).first);
    using ValueOut = decltype(merge_12(df1.tags[0], df2.tags[0], df1.values[0], df2.values[0]).second);
    DataFrame<TagOut, ValueOut> df_out;

    size_t i1 = 0, i2 = 0;

    while (i2 < df2.size())
    {
        // Process tags in df1 that are smaller than the current tag in df2.
        while ((df1.tags[i1] < df2.tags[i2]) && (i1 < df1.size()))
        {
            auto acc = merge_12(df1.tags[i1], NoTag(), df1.values[i1], NoValue());
            auto tag1 = df1.tags[i1];

            i1++;

            // Combine the run of identical tags from df1.
            for (; df1.tags[i1] == tag1; ++i1)
                acc = accumulate(df1.tags[i1], NoTag(), df1.values[i1], acc.second);

            df_out.push_back(acc);
        }

        // Process tags in df1 that match the current tag in df2.
        if (df1.tags[i1] == df2.tags[i2])
        {
            auto acc = merge_12(df1.tags[i1], df2.tags[i2], df1.values[i1], df2.values[i2]);

            i1++;

            // Combine the run of identical tags from df1.
            for (; (df1.tags[i1] == df2.tags[i2]) && (i1 < df1.size()); ++i1)
                acc = accumulate(df1.tags[i1], df2.tags[i2], df1.values[i1], acc.second);

            // Push the result for all repeated tags in df2.
            for (auto tag2 = df2.tags[i2]; (i2 < df2.size()) && (df2.tags[i2] == tag2); ++i2)
                df_out.push_back(acc);
        }
        else
        {
            // df1 had no tags that matched df2.
            ++i2;
        }
    }

    // Process remaining tags in df1.
    while (i1 < df1.size())
    {
        auto acc = merge_12(df1.tags[i1], NoTag(), df1.values[i1], NoValue());

        i1++;

        // Combine the run of identical tags from df1.
        for (auto tag1 = df1.tags[i1]; (tag1 == df1.tags[i1]) && (i1 < df1.size()); ++i1)
            acc = accumulate(df1.tags[i1], NoTag(), df1.values[i1], acc.second);

        df_out.push_back(acc);
    }

    return df_out;
}

template <typename ReductionOp, typename Tag, typename Value1, typename Value2>
struct ReductionAdaptor
{
    ReductionOp op;

    using ValueAccumulation = decltype(op(Value1(), Value2()));

    ReductionAdaptor(ReductionOp _op, Tag, Value1, Value2) : op(_op) {}

    // By default, joining generates a NoTag
    template <typename T1, typename T2, typename V1, typename V2>
    auto operator()(T1, T2, V1 v1, V2)
    {
        return std::pair(NoTag(), v1);
    }

    // Tags match and v1 and v2 have the expected type.
    template <>
    auto operator()(Tag tag1, Tag, Value1 v1, Value2 v2)
    {
        return std::pair(tag1, op(v1, v2));
    }

    // Tags match and v1 has the expected type and v2 is the accumulation type
    template <>
    auto operator()(Tag tag1, Tag, Value1 v1, ValueAccumulation va)
    {
        return std::pair(tag1, op(v1, va));
    }
};

// Specialize the reduction adaptor to when DataFrame2 is a NoValue dataframe.
template <typename ReductionOp, typename Tag, typename Value1>
struct ReductionAdaptor<ReductionOp, Tag, Value1, NoValue>
{
    ReductionOp op;

    using ValueAccumulation = decltype(op(Value1(), Value1()));

    ReductionAdaptor(ReductionOp _op, Tag, Value1, NoValue) : op(_op) {}

    // By default, joining generates a NoTag
    template <typename T1, typename T2, typename V1, typename V2>
    auto operator()(T1, T2, V1 v1, V2)
    {
        return std::pair(NoTag(), v1);
    }

    // Tags match and v1 has the expected type and v2 is the accumulation type
    template <>
    auto operator()(Tag tag1, Tag, Value1 v1, ValueAccumulation va)
    {
        return std::pair(tag1, op(v1, va));
    }

    // Tags match but v2 is NoValue().
    auto operator()(Tag tag1, Tag, Value1 v1, NoValue)
    {
        return std::pair(tag1, v1);
    }
};

template <typename CollateOp, typename Tag, typename Value1, typename Value2>
struct CollateAdaptor
{
    CollateOp op;

    using ValueAccumulation = decltype(op(Value1(), Value2()));

    CollateAdaptor(CollateOp _op, Tag, Value1, Value2) : op(_op) {}

    // The default behavior when combining rows is to not merge.
    template <typename T1, typename T2, typename V1, typename V2>
    auto operator()(T1, T2, V1, V2 v2)
    {
        return std::pair(NoTag(), v2);
    }

    // Combining a Value1 and a Value2 when tags match.
    template <>
    auto operator()(Tag tag1, Tag, Value1 v1, Value2 v2)
    {
        return std::pair(tag1, op(v1, v2));
    }

    // Accumulating results does nothing.
    template <>
    auto operator()(Tag tag1, Tag, Value1 v1, ValueAccumulation acc)
    {
        return std::pair(tag1, acc);
    }
};

struct Join
{
    template <typename Tag, typename Value1, typename Value2>
    static auto sum(const DataFrame<Tag, Value1> &df1, const DataFrame<Tag, Value2> &df2)
    {
        auto collate_op = ReductionAdaptor(
            [](Value1 v1, Value2 v2)
            { return v1 + v2; },
            Tag(), Value1(), Value2());
        return merge(df1, df2, collate_op, collate_op);
    }

    template <typename Tag, typename Value1>
    static auto sum(const DataFrame<Tag, Value1> &df1, const DataFrame<Tag, NoValue> &df2)
    {
        auto collate_op = ReductionAdaptor(
            [](Value1 v1, Value1 va)
            { return v1 + va; },
            Tag(), Value1(), NoValue());
        return merge(df1, df2, collate_op, collate_op);
    }

    template <typename Tag, typename Value1, typename Value2, typename CollateOp>
    static auto collate(const DataFrame<Tag, Value1> &df1, const DataFrame<Tag, Value2> &df2, CollateOp op)
    {
        auto adapted_op = CollateAdaptor(op, Tag(), Value1(), Value2());
        return merge(df1, df2, adapted_op, adapted_op);
    }
};

/* Deduplicate the tags of a dataframe. Produces a tag-only dataframe (on that
 * has NoValue as its value type).
 */
template <typename Tag, typename Value>
DataFrame<Tag, NoValue> uniquify_tags(const DataFrame<Tag, Value> &df)
{
    // TODO: can i re-write this using merge() or collate()?
    DataFrame<Tag, NoValue> df_out;

    if (!df.size())
        return df_out;

    df_out.tags.push_back(df.tags[0]);
    for (size_t i = 1; i < df.size(); ++i)
        if (df.tags[i] != df_out.tags.back())
            df_out.tags.push_back(df.tags[i]);

    return df_out;
}

struct Group
{
    template <typename Tag, typename Value>
    static auto sum(const DataFrame<Tag, Value> &df)
    {
        return Join::sum(df, uniquify_tags(df));
    }
};