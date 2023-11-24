/*
Compile this demo with

    clang++ -Wall -std=c++2b  test_dataframe.cpp
*/
#include "dataframe.h"

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