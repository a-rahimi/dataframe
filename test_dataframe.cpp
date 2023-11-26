/*
Compile this demo with

   clang++ -Wall -std=c++2b  test_dataframe.cpp  -lgtest_main -lgtest
*/
#include <string>

#include <gtest/gtest.h>

#include "dataframe.h"

TEST(RangeTags, Simple)
{
    auto df = DataFrame<RangeTag, int>{.tags = {0, 5}, .values = {-1, -2, -3, -4, -5}};
    auto i = DataFrame<size_t, NoValue>{{2, 3}};
    auto c = df[i];

    EXPECT_EQ(c.tags, (std::vector<size_t>{2, 3}));
    EXPECT_EQ(c.values, (std::vector<int>{-3, -4}));
}

TEST(Columnar, Simple)
{
    auto tags = std::vector<std::string>{"ali", "john"};

    struct Columnar
    {
        DataFrame<std::string, std::string> favorite_color;
        DataFrame<std::string, int> num_toes;
        DataFrame<std::string, int> num_teeth;
    } df{
        {tags, {"green", "blue"}},
        {tags, {6, 10}},
        {tags, {18, 32}}};

    auto toes_per_tooth = Join::collate(df.num_toes, df.num_teeth, [](int num_toes, int num_teeth)
                                        { return float(num_toes) / num_teeth; });
    EXPECT_EQ(toes_per_tooth.size(), 2);
    EXPECT_EQ(toes_per_tooth.values[0], 6.f / 18);
    EXPECT_EQ(toes_per_tooth.values[1], 10.f / 32);
}

struct O1
{
    std::string favorite_color;
    int num_toes;
};
struct O2
{
    int num_teeth;
};

struct O3 : O1, O2
{
    O3(const O1 &o1, const O2 &o2) : O1(o1), O2(o2) {}
};

bool operator==(const O3 &left, const O3 &right)
{
    return (left.favorite_color == right.favorite_color) && (left.num_toes == right.num_toes) && (left.num_teeth == right.num_teeth);
}

std::ostream &operator<<(std::ostream &s, const O3 &o3)
{
    s << "O3(" << o3.favorite_color << ", " << o3.num_toes << ", " << o3.num_teeth << ')';
    return s;
}

TEST(Join, Structs)
{
    auto df1 = DataFrame<std::string, O1>{
        {"ali", "john"},
        {O1{.favorite_color = "green", .num_toes = 6}, O1{.favorite_color = "blue", .num_toes = 10}}};

    auto df2 = DataFrame<std::string, O2>{
        {"ali", "john"},
        {O2{.num_teeth = 18}, O2{.num_teeth = 32}}};

    auto g = Join::collate(df1, df2,
                           [](const O1 &left, const O2 &right)
                           { return O3(left, right); });

    EXPECT_EQ(g.tags, df1.tags);
    EXPECT_EQ(g.values[0], O3(O1{"green", 6}, O2{18}));
    EXPECT_EQ(g.values[1], O3(O1{"blue", 10}, O2{32}));
}

TEST(Join, Strings)
{
    auto df1 = DataFrame<std::string, float>{{"ali", "john"}, {1., 2.}};
    auto df2 = DataFrame<std::string, float>{{"ali", "john"}, {10., 20.}};

    auto g = Join::sum(df1, df2);

    EXPECT_EQ(g.tags, df1.tags);
    EXPECT_EQ(g.values, std::vector<float>({11., 22.}));
}

TEST(Join, Simple)
{
    auto df1 = DataFrame<int, float>{.tags = {1, 2, 3}, .values = {10., 20., 30.}};
    auto df2 = DataFrame<int, float>{.tags = {1, 2, 3}, .values = {-11., -22., -33.}};

    auto g = Join::sum(df1, df2);

    EXPECT_EQ(g.size(), 3);

    EXPECT_EQ(g.tags, (std::vector<int>{1, 2, 3}));
    EXPECT_EQ(g.values, (std::vector<float>{-1., -2., -3.}));
}

TEST(Join, DuplicatesLeft)
{
    auto df1 = DataFrame<int, float>{{1, 2, 2, 3}, {10., 20., 100., 30.}};
    auto df2 = DataFrame<int, float>{{1, 2, 3}, {-11., -22., -33.}};

    auto g = Join::sum(df1, df2);

    EXPECT_EQ(g.size(), 3);

    EXPECT_EQ(g.tags, (std::vector<int>{1, 2, 3}));
    EXPECT_EQ(g.values, (std::vector<float>{-1., 98., -3.}));
}
TEST(Join, Binary)
{
    auto df1 = DataFrame<int, float>{.tags = {0, 0, 1}, .values = {10., 20., 30.}};
    auto df2 = DataFrame<int, float>{.tags = {0, 1, 1}, .values = {-11., -22., -33.}};

    auto g = Join::sum(df1, df2);

    EXPECT_EQ(g.size(), 3);
    EXPECT_EQ(g.tags, (std::vector<int>{0, 1, 1}));
    EXPECT_EQ(g.values, (std::vector<float>{19., 8., 8.}));
}

TEST(Join, SimplePair)
{
    auto df1 = DataFrame<int, float>{{1, 2, 3}, {10., 20., 30.}};
    auto df2 = DataFrame<int, float>{{1, 2, 3}, {-11., -22., -33.}};

    auto g = Join::collate(df1, df2,
                           [](float v1, float v2)
                           { return std::pair(v1, v2); });

    EXPECT_EQ(g.size(), 3);
    EXPECT_EQ(g.tags, (std::vector<int>{1, 2, 3}));
    EXPECT_EQ(g.values[0], std::pair(10.f, -11.f));
    EXPECT_EQ(g.values[1], std::pair(20.f, -22.f));
    EXPECT_EQ(g.values[2], std::pair(30.f, -33.f));
}

TEST(Utilities, first_tags)
{
    auto df = DataFrame<int, float>{{1, 2, 2, 3}, {10., 20., 100., 30.}};
    auto dft = uniquify_tags(df);

    EXPECT_TRUE((std::is_same<decltype(dft.values[100]), NoValue>::value));
    EXPECT_EQ(dft.size(), 3);

    EXPECT_EQ(dft.tags, (std::vector<int>{1, 2, 3}));
}

TEST(Group, sum)
{
    auto df = DataFrame<int, float>{{1, 2, 2, 3}, {10., 20., 100., 30.}};

    auto g = Group::sum(df);

    EXPECT_EQ(g.tags, (std::vector<int>{1, 2, 3}));
    EXPECT_EQ(g.values, (std::vector<float>{10., 120., 30.}));
}

TEST(Index, Simple)
{
    auto df = DataFrame<int, float>{.tags = {1, 2, 3, 4}, .values = {10., 20., 30., 40.}};

    auto i = DataFrame<int, float>{.tags = {2, 3}, .values = {-20., -30.}};

    auto g = df[i];

    EXPECT_EQ(g.size(), 2);

    EXPECT_EQ(g.tags, (std::vector<int>{2, 3}));
    EXPECT_EQ(g.values, (std::vector<float>{20., 30.}));
}

TEST(Index, NoValues)
{
    auto df = DataFrame<int, float>{.tags = {1, 2, 3, 4}, .values = {10., 20., 30., 40.}};

    auto i = DataFrame<int, NoValue>{.tags = {2, 3}};

    auto g = df[i];

    EXPECT_EQ(g.size(), 2);

    EXPECT_EQ(g.tags, (std::vector<int>{2, 3}));
    EXPECT_EQ(g.values, (std::vector<float>{20., 30.}));
}