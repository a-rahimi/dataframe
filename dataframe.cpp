/*
Compile this demo with

    clang++ -Wall -std=c++2b  dataframe.cpp
*/

#include <cassert>
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
    auto operator()(T tag, TO tag_other, V v, VO)
    {
        return std::pair(NoTag(), v);
    }

    // A successful indexing reduction takes the value of `this`, and ignores
    // the value of the index.
    template <typename ValueOther>
    auto operator()(Tag tag, Tag tag_other, Value v, ValueOther)
    {
        return std::pair(tag, v);
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

struct IndexRange
{
    size_t start, end, step;

    size_t size() const { return (end - start) / step; }
    size_t operator[](size_t i) const { return start + i * step; }
};

struct RangeTag;

// A dataframe with range tags.
template <typename Value>
struct DataFrame<RangeTag, Value>
{
    std::vector<Value> values;
    IndexRange tags;

    size_t size() const { return values.size(); }
};

// A dataframe with no values. Just tags. These are useful for indexing
// operations, for example.
template <typename Tag>
struct DataFrame<Tag, NoValue>
{
    std::vector<Tag> tags;
    struct PlaceHolderVector
    {
        NoValue operator[](size_t i) const { return NoValue(); }
    } values;

    size_t size() const { return tags.size(); }
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

template <typename Merge12Op, typename AccumulateOp, typename Tag, typename Value1, typename Value2>
static auto
merge(const DataFrame<Tag, Value1> &df1, const DataFrame<Tag, Value2> &df2, Merge12Op merge_12, AccumulateOp accumulate)
{
    auto df_out = DataFrame(Tag(), merge_12(Tag(), Tag(), Value1(), Value2()).second);

    size_t i1 = 0, i2 = 0;

    while (i2 < df2.size())
    {
        // Process tags in df1 that are smaller than the current tag in df2.
        while ((df1.tags[i1] < df2.tags[i2]) && (i1 < df1.size()))
        {
            auto acc = merge_12(df1.tags[i1], NoTag(), df1.values[i1], NoValue());
            Tag tag1 = df1.tags[i1];

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
        auto acc = merge_12(df1.tags[i1], NoTag(), df1.values[i1], NoValue());

        i1++;

        // Combine the run of identical tags from df1.
        for (Tag tag1 = df1.tags[i1]; (tag1 == df1.tags[i1]) && (i1 < df1.size()); ++i1)
            acc = accumulate(df1.tags[i1], NoTag(), df1.values[i1], acc.second);

        df_out.push_back(acc);
    }

    return df_out;
}

template <typename Tag, typename Value1, typename Value2>
struct JoinSumOp
{
    // By default, joining generates a NoTag
    template <typename T1, typename T2, typename V1, typename V2>
    auto operator()(T1 tag1, T2 tag2, V1 v1, V2 v2)
    {
        return std::pair(NoTag(), v1);
    }
    // Tags match and v1 and v2 have the expected type.
    auto operator()(Tag tag1, Tag tag2, Value1 v1, Value2 v2)
    {
        return std::pair(tag1, v1 + v2);
    }
};

template <typename Tag, typename Value1>
struct JoinSumOp<Tag, Value1, NoValue>
{
    // By default, joining generates a NoTag
    template <typename T1, typename T2, typename V1, typename V2>
    auto operator()(T1 tag1, T2 tag2, V1 v1, V2 v2)
    {
        return std::pair(NoTag(), v1);
    }

    // Tags match but v2 is NoValue().
    auto operator()(Tag tag1, Tag tag2, Value1 v1, NoValue v2)
    {
        return std::pair(tag1, v1);
    }
};

template <typename Tag, typename Value1, typename Value2>
struct JoinPairOp
{
    // The default behavior when combining pairs.
    template <typename T1, typename T2, typename V1, typename V2>
    auto operator()(T1 tag1, T2 tag2, V1 v1, V2 v2)
    {
        return std::pair(NoTag(), v2);
    }

    // Combining a Value1 and a Value2.
    template <>
    auto operator()(Tag tag1, Tag tag2, Value1 v1, Value2 v2)
    {
        return std::pair(tag1, std::pair(v1, v2));
    }

    // Combining a Pair with another Pair results in a flat pair. Notably, it does not result
    template <>
    auto operator()(Tag tag1, Tag tag2, Value1 v1, std::pair<Value1, Value2> v2)
    {
        return std::pair(tag1, std::pair(v1, v2.second));
    }
};

struct Join
{
    template <typename Tag, typename Value1, typename Value2>
    static auto sum(const DataFrame<Tag, Value1> &df1, const DataFrame<Tag, Value2> &df2)
    {
        return merge(df1, df2, JoinSumOp<Tag, Value1, Value2>(), JoinSumOp<Tag, Value1, Value1>());
    }

    template <typename Tag, typename Value1, typename Value2>
    static auto pair(const DataFrame<Tag, Value1> &df1, const DataFrame<Tag, Value2> &df2)
    {
        return merge(df1, df2, JoinPairOp<Tag, Value1, Value2>(), JoinPairOp<Tag, Value1, Value2>());
    }
};

template <typename Tag, typename Value>
auto first_tags(const DataFrame<Tag, Value> &df)
{
    auto df_out = DataFrame<Tag, NoValue>();

    if (!df.size())
        return df_out;

    df_out.tags.push_back(df.tags[0]);
    for (size_t i = 1; i < df.size(); ++i)
    {
        if (df.tags[i] != df_out.tags.back())
            df_out.tags.push_back(df.tags[i]);
    }

    return df_out;
}

struct Group
{
    template <typename Tag, typename Value>
    static auto sum(const DataFrame<Tag, Value> &df)
    {
        return Join::sum(df, first_tags(df));
    }
};

void test_first_tags()
{
    auto df = DataFrame<int, float>{{1, 2, 2, 3}, {10., 20., 100., 30.}};
    auto dft = first_tags(df);

    assert((std::is_same<decltype(dft.values[100]), NoValue>::value));
    assert(dft.size() == 3);

    assert(dft.tags[0] == 1);
    assert(dft.tags[1] == 2);
    assert(dft.tags[2] == 3);
}

void test_group_sum()
{
    auto df = DataFrame<int, float>{{1, 2, 2, 3}, {10., 20., 100., 30.}};

    auto g = Group::sum(df);

    assert(g.tags[0] == 1);
    assert(g.values[0] == 10.f);
    assert(g.tags[1] == 2);
    assert(g.values[1] == 120.f);
    assert(g.tags[2] == 3);
    assert(g.values[2] == 30.);
}

void test_join_simple()
{
    auto df1 = DataFrame<int, float>{{1, 2, 3}, {10., 20., 30.}};
    auto df2 = DataFrame<int, float>{{1, 2, 3}, {-11., -22., -33.}};

    auto g = Join::sum(df1, df2);

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

    auto g = Join::sum(df1, df2);

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

    auto g = Join::sum(df1, df2);

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

    auto g = Join::pair(df1, df2);

    assert(g.size() == 3);
    assert(g.tags[0] == 1);
    assert(g.values[0] == std::pair(10.f, -11.f));
    assert(g.tags[1] == 2);
    assert(g.values[1] == std::pair(20.f, -22.f));
    assert(g.tags[2] == 3);
    assert(g.values[2] == std::pair(30.f, -33.f));
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

void test_index_no_values()
{
    auto df = DataFrame<int, float>{{1, 2, 3, 4}, {10., 20., 30., 40.}};

    auto i = DataFrame<int, NoValue>{{2, 3}};

    auto g = df[i];

    assert(g.size() == 2);

    assert(g.tags[0] == 2);
    assert(g.values[0] == 20.f);
    assert(g.tags[1] == 3);
    assert(g.values[1] == 30.f);
}

int main()
{
    test_first_tags();
    test_group_sum();
    test_index_no_values();
    test_join_simple_pair();
    test_join_duplicates_left();
    test_index();
    test_join_simple();
    test_join_binary();
}