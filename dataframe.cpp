/*
Compile this demo with

   clang++ -Wall -std=c++2b  dataframe.cpp
*/

#include <cassert>
#include <initializer_list>
#include <iostream>
#include <utility>
#include <vector>

struct NoTag
{
};

struct NoValue
{
};

template <typename Tag, typename Value, typename ValueOther>
struct IndexReduceOp
{
    // Indexing fails if the tag or values don't have the right type.
    template <typename T, typename TO, typename V, typename VO>
    auto operator()(T tag, TO tag_other, V v, VO v_other)
    {
        return std::make_pair(NoTag(), v);
    }

    // A successful indexing reduction takes the value of `this`, and ignores
    // the value of the index.
    template <>
    auto operator()(Tag tag, Tag tag_other, Value v, ValueOther v_other)
    {
        return std::make_pair(tag, v);
    }
};

template <typename T1, typename T2>
std::ostream &operator<<(std::ostream &s, const std::pair<T1, T2> &p)
{
    s << '(' << p.first << ", " << p.second << ')';
    return s;
}

template <typename Tag, typename Value>
struct DataFrame
{
    std::vector<Tag> tags;
    std::vector<Value> values;

    DataFrame(Tag, Value) {}
    DataFrame(std::initializer_list<Tag> t, std::initializer_list<Value> v) : tags(t), values(v) {}

    size_t size() const { return tags.size(); }

    template <typename ValueOther>
    DataFrame<Tag, Value> operator[](const DataFrame<Tag, ValueOther> &index)
    {
        return merge(*this, index, Value(), IndexReduceOp<Tag, Value, ValueOther>());
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
    {
        auto r = df[i];
        s << r.first << '\t' << r.second << std::endl;
    }
    return s;
}

struct IndexRange
{
    size_t start, end, step;

    size_t size() const { return (end - start) / step; }
    size_t operator[](size_t i) const { return start + i * step; }
};

struct RangeTag;

template <typename Value>
struct DataFrame<RangeTag, Value>
{
    std::vector<Value> values;
    IndexRange tags;

    size_t size() const { return values.size(); }
};

template <typename Op, typename Tag, typename Value1, typename Value2, typename Tout>
static auto
merge(const DataFrame<Tag, Value1> &df1, const DataFrame<Tag, Value2> &df2, Tout init, Op merge_elements)
{
    auto df_out = DataFrame(Tag(), merge_elements(Tag(), Tag(), Value1(), Value2()).second);

    size_t i1 = 0, i2 = 0;

    while (i2 < df2.size())
    {
        // Process tags in df1 that are smaller than the current tag in df2.
        while ((df1.tags[i1] < df2.tags[i2]) && (i1 < df1.size()))
        {
            auto acc = merge_elements(df1.tags[i1], NoTag(), df1.values[i1], init);
            Tag tag1 = df1.tags[i1];

            i1++;

            // Combine the run of identical tags from df1.
            for (; df1.tags[i1] == tag1; ++i1)
                acc = merge_elements(df1.tags[i1], NoTag(), df1.values[i1], acc.second);

            df_out.push_back(acc);
        }

        // Process tags in df1 that match the current tag in df2.
        if (df1.tags[i1] == df2.tags[i2])
        {
            auto acc = merge_elements(df1.tags[i1], df2.tags[i2], df1.values[i1], df2.values[i2]);

            i1++;

            // Combine the run of identical tags from df1.
            for (; (df1.tags[i1] == df2.tags[i2]) && (i1 < df1.size()); ++i1)
                acc = merge_elements(df1.tags[i1], df2.tags[i2], df1.values[i1], acc.second);

            // Push the result for all repeated tags in df2.
            for (Tag tag2 = df2.tags[i2]; (i2 < df2.size()) && (df2.tags[i2] == tag2); ++i2)
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
        auto acc = merge_elements(df1.tags[i1], NoTag(), df1.values[i1], init);

        i1++;

        // Combine the run of identical tags from df1.
        for (Tag tag1 = df1.tags[i1]; (tag1 == df1.tags[i1]) && (i1 < df1.size()); ++i1)
            acc = merge_elements(df1.tags[i1], NoTag(), df1.values[i1], acc.second);

        df_out.push_back(acc);
    }

    return df_out;
}

template <typename ReduceOp>
struct JoinReductionOpAdaptor
{
    ReduceOp op;
    JoinReductionOpAdaptor(ReduceOp _op) : op(_op) {}

    template <typename Tag, typename Value1, typename Value2>
    auto operator()(Tag tag1, NoTag tag2, Value1 v1, Value2 v2)
    {
        return std::make_pair(NoTag(), Value1());
    }
    template <typename Tag, typename Value1, typename Value2>
    auto operator()(NoTag tag1, Tag tag2, Value1 v1, Value2 v2)
    {
        return std::make_pair(NoTag(), Value1());
    }

    template <typename Tag, typename Value1>
    auto operator()(Tag tag1, Tag tag2, Value1 v1, NoValue v)
    {
        return std::make_pair(NoTag(), v);
    }

    template <typename Tag, typename Value1, typename Value2>
    auto operator()(Tag tag1, Tag tag2, Value1 v1, Value2 v2)
    {
        assert(tag1 == tag2);
        return std::make_pair(tag1, op(v1, v2));
    }
};

template <typename Tag, typename Value1, typename Value2>
struct JoinPairOp
{
    // The default behavior when combining pairs.
    template <typename T1, typename T2, typename V1, typename V2>
    auto operator()(T1 tag1, T2 tag2, V1 v1, V2 v2)
    {
        return std::make_pair(NoTag(), v2);
    }

    // Combining a Value1 and a Value2.
    template <>
    auto operator()(Tag tag1, Tag tag2, Value1 v1, Value2 v2)
    {
        return std::make_pair(tag1, std::pair<Value1, Value2>(v1, v2));
    }

    // Combining a Pair with another Pair results in a flat pair. Notably, it does not result
    template <>
    auto operator()(Tag tag1, Tag tag2, Value1 v1, std::pair<Value1, Value2> v2)
    {
        return std::make_pair(tag1, std::pair<Value1, Value2>(v1, v2.second));
    }
};

template <typename Tag, typename Value1, typename Value2>
struct Join
{
    const DataFrame<Tag, Value1> &df1;
    const DataFrame<Tag, Value2> &df2;

    Join(const DataFrame<Tag, Value1> &_df1, const DataFrame<Tag, Value2> &_df2) : df1(_df1), df2(_df2) {}

    auto sum()
    {
        return merge(df1, df2, Value1(0), JoinReductionOpAdaptor([](Value1 v1, Value2 v2)
                                                                 { return v1 + v2; }));
    }

    auto pair()
    {
        return merge(df1, df2, std::pair<Value1, Value2>(), JoinPairOp<Tag, Value1, Value2>());
    }
};

void test_join_simple()
{
    auto df1 = DataFrame<int, float>{{1, 2, 3}, {10., 20., 30.}};
    auto df2 = DataFrame<int, float>{{1, 2, 3}, {-11., -22., -33.}};

    auto g = Join(df1, df2).sum();

    assert(g.size() == 3);

    assert(g.tags[0] == 1);
    assert(g.values[0] == -1.f);
    assert(g.tags[1] == 2);
    assert(g.values[1] == -2.f);
    assert(g.tags[2] == 3);
    assert(g.values[2] == -3.f);
}

void test_join_duplicates_left()
{
    auto df1 = DataFrame<int, float>{{1, 2, 2, 3}, {10., 20., 100., 30.}};
    auto df2 = DataFrame<int, float>{{1, 2, 3}, {-11., -22., -33.}};

    auto g = Join(df1, df2).sum();

    assert(g.size() == 3);

    assert(g.tags[0] == 1);
    assert(g.values[0] == -1.f);
    assert(g.tags[1] == 2);
    assert(g.values[1] == 98.f);
    assert(g.tags[2] == 3);
    assert(g.values[2] == -3.f);
}

void test_join_binary()
{
    auto df1 = DataFrame<int, float>{{0, 0, 1}, {10., 20., 30.}};
    auto df2 = DataFrame<int, float>{{0, 1, 1}, {-11., -22., -33.}};

    auto g = Join(df1, df2).sum();

    assert(g.size() == 3);
    assert(g.tags[0] == 0);
    assert(g.values[0] == 19.f);
    assert(g.tags[1] == 1);
    assert(g.values[1] == 8.f);
    assert(g.tags[2] == 1);
    assert(g.values[2] == 8.f);
}

void test_join_simple_pair()
{
    auto df1 = DataFrame<int, float>{{1, 2, 3}, {10., 20., 30.}};
    auto df2 = DataFrame<int, float>{{1, 2, 3}, {-11., -22., -33.}};

    auto g = Join(df1, df2).pair();

    assert(g.size() == 3);
    assert(g.tags[0] == 1);
    assert(g.values[0] == std::make_pair(10.f, -11.f));
    assert(g.tags[1] == 2);
    assert(g.values[1] == std::make_pair(20.f, -22.f));
    assert(g.tags[2] == 3);
    assert(g.values[2] == std::make_pair(30.f, -33.f));
}

void test_index()
{
    auto df = DataFrame<int, float>{{1, 2, 3, 4}, {10., 20., 30., 40.}};

    auto i = DataFrame<int, float>{{2, 3}, {-20., -30.}};

    auto g = df[i];

    assert(g.size() == 2);

    assert(g.tags[0] == 2);
    assert(g.values[0] == 20.f);
    assert(g.tags[1] == 3);
    assert(g.values[1] == 30.f);
}

int main()
{
    test_join_simple_pair();
    test_join_duplicates_left();
    test_index();
    test_join_simple();
    test_join_binary();
}