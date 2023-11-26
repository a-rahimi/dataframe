/*
Compile this demo with

    clang++ -Wall -std=c++2b  test_dataframe.cpp
*/
#include "dataframe.h"
#include <string>

void test_range_tags()
{
    auto df = DataFrame<RangeTag, int>{.tags = {0, 5}, .values = {-1, -2, -3, -4, -5}};
    auto i = DataFrame<size_t, NoValue>{{2, 3}};
    auto c = df[i];

    assert((c.tags == std::vector<size_t>{2, 3}));
    assert((c.values == std::vector<int>{-3, -4}));
}

void test_columnar()
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
    assert(toes_per_tooth.size() == 2);
    assert(toes_per_tooth.values[0] == 6.f / 18);
    assert(toes_per_tooth.values[1] == 10.f / 32);
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

void test_join_structs()
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

    assert(g.tags == df1.tags);
    assert(g.values[0] == O3(O1{"green", 6}, O2{18}));
    assert(g.values[1] == O3(O1{"blue", 10}, O2{32}));
}

void test_join_strings()
{
    auto df1 = DataFrame<std::string, float>{{"ali", "john"}, {1., 2.}};
    auto df2 = DataFrame<std::string, float>{{"ali", "john"}, {10., 20.}};

    auto g = Join::sum(df1, df2);

    assert(g.tags == df1.tags);
    assert(g.values == std::vector<float>({11., 22.}));
}

void test_first_tags()
{
    auto df = DataFrame<int, float>{{1, 2, 2, 3}, {10., 20., 100., 30.}};
    auto dft = uniquify_tags(df);

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
    auto df1 = DataFrame<int, float>{.tags = {1, 2, 3}, .values = {10., 20., 30.}};
    auto df2 = DataFrame<int, float>{.tags = {1, 2, 3}, .values = {-11., -22., -33.}};

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
    auto df1 = DataFrame<int, float>{.tags = {0, 0, 1}, .values = {10., 20., 30.}};
    auto df2 = DataFrame<int, float>{.tags = {0, 1, 1}, .values = {-11., -22., -33.}};

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

    auto g = Join::collate(df1, df2,
                           [](float v1, float v2)
                           { return std::pair(v1, v2); });

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
    auto df = DataFrame<int, float>{.tags = {1, 2, 3, 4}, .values = {10., 20., 30., 40.}};

    auto i = DataFrame<int, float>{.tags = {2, 3}, .values = {-20., -30.}};

    auto g = df[i];

    assert(g.size() == 2);

    assert(g.tags[0] == 2);
    assert(g.values[0] == 20.f);
    assert(g.tags[1] == 3);
    assert(g.values[1] == 30.f);
}

void test_index_no_values()
{
    auto df = DataFrame<int, float>{.tags = {1, 2, 3, 4}, .values = {10., 20., 30., 40.}};

    auto i = DataFrame<int, NoValue>{.tags = {2, 3}};

    auto g = df[i];

    assert(g.size() == 2);

    assert(g.tags[0] == 2);
    assert(g.values[0] == 20.f);
    assert(g.tags[1] == 3);
    assert(g.values[1] == 30.f);
}

int main()
{
    test_range_tags();
    test_columnar();
    test_join_structs();
    test_join_strings();
    test_first_tags();
    test_group_sum();
    test_index_no_values();
    test_join_simple_pair();
    test_join_duplicates_left();
    test_index();
    test_join_simple();
    test_join_binary();
}