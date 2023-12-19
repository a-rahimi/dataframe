/*
Compile this demo with

   clang++ -Wall -std=c++2b  test_dataframe2.cpp  -lgtest_main -lgtest
*/

#include <gtest/gtest.h>

#include <string>

#include "dataframe.h"

TEST(DataFrame, copy) {
    auto df = DataFrame<int, float>({1, 2, 3, 4}, {10., 20., 30., 40.});
    auto df_copy = df;

    EXPECT_EQ(df.tags, df_copy.tags);
    EXPECT_EQ(df.values, df_copy.values);
    EXPECT_EQ(*df.tags, *df_copy.tags);
    EXPECT_EQ(*df.values, *df_copy.values);
}

void modify_df(DataFrame<int, float> df) { (*df.values)[2] = 35.; }

TEST(DataFrame, passed_by_reference) {
    auto df = DataFrame<int, float>({1, 2, 3, 4}, {10., 20., 30., 40.});

    EXPECT_EQ((*df.values)[2], 30.);
    modify_df(df);
    EXPECT_EQ((*df.values)[2], 35.);
}

TEST(DataFrame, scalar_index_TagValue) {
    auto df = DataFrame<int, float>({1, 2, 3, 4}, {10., 20., 30., 40.});

    EXPECT_EQ(df[3].t, 4);
    EXPECT_EQ(df[3].v, 40.);
}

TEST(DataFrame, scalar_index_TagValue_modifiable) {
    auto df = DataFrame<int, float>({1, 2, 3, 4}, {10., 20., 30., 40.});

    df[1].v = 21.;

    EXPECT_EQ(df[1].v, 21.);
}

TEST(DataFrame, scalar_index_TagValue_const) {
    const DataFrame<int, float> df({1, 2, 3, 4}, {10., 20., 30., 40.});

    EXPECT_EQ(df[1].v, 20.);
}

TEST(Expr_DataFrame, find_tag) {
    auto df = DataFrame<int, float>({1, 2, 3, 4}, {10., 20., 30., 40.});
    auto edf = to_expr(df);

    edf.advance_to_tag(2);

    EXPECT_FALSE(edf.end());
    EXPECT_EQ(edf.tag, 2);
    EXPECT_EQ(edf.value, 20);
    EXPECT_EQ(df[edf.i].t, 2);
    EXPECT_EQ(df[edf.i].v, 20);
}

TEST(Expr_DataFrame, find_tag_missing) {
    auto df = DataFrame<int, float>({1, 2, 3, 4}, {10., 20., 30., 40.});
    auto edf = to_expr(df);

    edf.advance_to_tag(20);

    EXPECT_TRUE(edf.end());
}

TEST(Expr_DataFrame, materialize) {
    DataFrame<int, float> original_df({1, 2, 3, 4}, {10., 20., 30., 40.});
    auto edf = Expr_DataFrame(original_df);

    auto df = edf.materialize();

    EXPECT_NE(df.tags, original_df.tags);
    EXPECT_NE(df.values, original_df.values);
    EXPECT_EQ(*df.tags, *original_df.tags);
    EXPECT_EQ(*df.values, *original_df.values);
}

TEST(Index, Simple) {
    auto df = DataFrame<int, float>{
        {1,   2,   3,   4  },
        {10., 20., 30., 40.}
    };

    auto i = DataFrame<int, float>{
        {2,    3   },
        {-20., -30.}
    };

    auto g = df[i].materialize();

    EXPECT_EQ(g.size(), 2);
    EXPECT_EQ(*g.tags, (std::vector<int>{2, 3}));
    EXPECT_EQ(*g.values, (std::vector<float>{20., 30.}));
}

TEST(Index, NoValues) {
    auto df = DataFrame<int, float>({1, 2, 3, 4}, {10., 20., 30., 40.});

    auto i = DataFrame<int, NoValue>({2, 3}, {});

    auto g = df[i].materialize();

    EXPECT_EQ(g.size(), 2);

    EXPECT_EQ(*g.tags, (std::vector<int>{2, 3}));
    EXPECT_EQ(*g.values, (std::vector<float>{20., 30.}));
}

TEST(Reduce, sum) {
    auto df = DataFrame<int, float>({1, 2, 2, 3}, {10., 20., 100., 30.});

    auto g = df.reduce_sum().materialize();

    EXPECT_EQ(*g.tags, (std::vector<int>{1, 2, 3}));
    EXPECT_EQ(*g.values, (std::vector<float>{10., 120., 30.}));
}

TEST(Reduce, max) {
    auto df = DataFrame<int, float>{
        {1,   2,   2,    3  },
        {10., 20., 100., 30.}
    };

    auto g = df.reduce_max().materialize();

    EXPECT_EQ(*g.tags, (std::vector<int>{1, 2, 3}));
    EXPECT_EQ(*g.values, (std::vector<float>{10., 100., 30.}));
}

TEST(Reduce, count) {
    auto df = DataFrame<int, float>{
        {1,   2,   2,    3  },
        {10., 20., 100., 30.}
    };

    auto g = df.reduce_count().materialize();

    EXPECT_EQ(*g.tags, (std::vector<int>{1, 2, 3}));
    EXPECT_EQ(*g.values, (std::vector<size_t>{1, 2, 1}));
}

TEST(Reduce, max_manual) {
    auto df = DataFrame<int, float>({1, 2, 2, 3}, {10., 20., 100., 30.});

    auto g = df.reduce([](float a, float b) { return a > b ? a : b; }, [](float a) { return a; }).materialize();

    EXPECT_EQ(*g.tags, (std::vector<int>{1, 2, 3}));
    EXPECT_EQ(*g.values, (std::vector<float>{10., 100., 30.}));
}

TEST(Reduce, moments) {
    auto df = DataFrame<int, float>({1, 2, 2, 3}, {10., 1., 2., 30.});

    auto g = df.reduce_moments().materialize();

    EXPECT_EQ(*g.tags, (std::vector<int>{1, 2, 3}));
    EXPECT_EQ(g[0].v.count, 1);
    EXPECT_EQ(g[1].v.count, 2);
    EXPECT_EQ(g[2].v.count, 1);

    EXPECT_EQ(g[0].v.sum, 10.);
    EXPECT_EQ(g[1].v.sum, 3.);
    EXPECT_EQ(g[2].v.sum, 30.);

    EXPECT_EQ(g[0].v.sum_squares, 100.);
    EXPECT_EQ(g[1].v.sum_squares, 5.);
    EXPECT_EQ(g[2].v.sum_squares, 900.);

    EXPECT_EQ(g[0].v.mean(), 10.);
    EXPECT_EQ(g[1].v.mean(), 1.5);
    EXPECT_EQ(g[2].v.mean(), 30.);

    EXPECT_EQ(g[0].v.var(), 0.);
    EXPECT_EQ(g[1].v.var(), .25);
    EXPECT_EQ(g[2].v.var(), 0.);
}

TEST(Reduce, moments_apply) {
    auto df = DataFrame<int, float>({1, 2, 2, 3}, {20., 2., 4., 60.});

    auto g = df.apply_to_values([](float v) { return v / 2; }).reduce_moments().materialize();

    EXPECT_EQ(*g.tags, (std::vector<int>{1, 2, 3}));
    EXPECT_EQ(g[0].v.count, 1);
    EXPECT_EQ(g[1].v.count, 2);
    EXPECT_EQ(g[2].v.count, 1);

    EXPECT_EQ(g[0].v.sum, 10.);
    EXPECT_EQ(g[1].v.sum, 3.);
    EXPECT_EQ(g[2].v.sum, 30.);

    EXPECT_EQ(g[0].v.sum_squares, 100.);
    EXPECT_EQ(g[1].v.sum_squares, 5.);
    EXPECT_EQ(g[2].v.sum_squares, 900.);
}

TEST(Apply, divide_by_2) {
    auto df = DataFrame<int, float>({1, 2, 2, 3}, {10., 20., 100., 30.});

    auto g = df.apply_to_values([](float v) { return v / 2; }).materialize();

    EXPECT_EQ(*g.tags, (std::vector<int>{1, 2, 2, 3}));
    EXPECT_EQ(*g.values, (std::vector<float>{5., 10., 50., 15.}));
}

TEST(Apply, find_tag) {
    auto df = DataFrame<int, float>({1, 2, 2, 3}, {10., 20., 100., 30.});

    auto expr = df.apply_to_values([](float v) { return v / 2; });
    expr.advance_to_tag(2);
    auto g = expr.materialize();

    EXPECT_EQ(*g.tags, (std::vector<int>{2, 2, 3}));
    EXPECT_EQ(*g.values, (std::vector<float>{10., 50., 15.}));
}

TEST(Apply, find_tag_last) {
    auto df = DataFrame<int, float>({1, 2, 2, 3}, {10., 20., 100., 30.});

    auto expr = df.apply_to_values([](float v) { return v / 2; });
    expr.advance_to_tag(3);
    auto g = expr.materialize();

    EXPECT_EQ(*g.tags, (std::vector<int>{3}));
    EXPECT_EQ(*g.values, (std::vector<float>{15.}));
}

TEST(Collate, SimplePair) {
    auto df1 = DataFrame<int, float>({1, 2, 3}, {10., 20., 30.});
    auto df2 = DataFrame<int, float>({1, 2, 3}, {-11., -22., -33.});

    auto g = df1.collate(df2, [](float v1, float v2) { return std::pair(v1, v2); }).materialize();

    EXPECT_EQ(g.size(), 3);
    EXPECT_EQ(*g.tags, (std::vector<int>{1, 2, 3}));
    EXPECT_EQ(g[0].v, std::pair(10.f, -11.f));
    EXPECT_EQ(g[1].v, std::pair(20.f, -22.f));
    EXPECT_EQ(g[2].v, std::pair(30.f, -33.f));
}

TEST(Collate, ReducedSum) {
    auto df1 = DataFrame<int, float>({0, 0, 1}, {10., 20., 30.});
    auto df2 = DataFrame<int, float>({0, 1, 1}, {-11., -22., -33.});

    auto g = df1.reduce_sum().collate_sum(df2.reduce_sum()).materialize();

    EXPECT_EQ(g.size(), 2);
    EXPECT_EQ(*g.tags, (std::vector<int>{0, 1}));
    EXPECT_EQ(*g.values, (std::vector<float>{19., -25.}));
}

TEST(Collate, ReducedLeftDuplicates) {
    auto df1 = DataFrame<int, float>({1, 2, 2, 3}, {10., 20., 100., 30.});
    auto df2 = DataFrame<int, float>({1, 2, 3}, {-11., -22., -33.});

    auto g = df1.reduce_sum().collate_sum(df2.reduce_sum()).materialize();

    EXPECT_EQ(g.size(), 3);

    EXPECT_EQ(*g.tags, (std::vector<int>{1, 2, 3}));
    EXPECT_EQ(*g.values, (std::vector<float>{-1., 98., -3.}));
}

TEST(Collate, Strings) {
    auto df1 = DataFrame<std::string, float>({"ali", "john"}, {1., 2.});
    auto df2 = DataFrame<std::string, float>({"ali", "john"}, {10., 20.});

    auto g = df1.collate_sum(df2).materialize();

    EXPECT_EQ(*g.tags, *df1.tags);
    EXPECT_EQ(*g.values, std::vector<float>({11., 22.}));
}

struct O1 {
    std::string favorite_color;
    int num_toes;
};
struct O2 {
    int num_teeth;
};

struct O3 : O1, O2 {
    O3() {}
    O3(const O1 &o1, const O2 &o2) : O1(o1), O2(o2) {}

    friend bool operator==(const O3 &left, const O3 &right) {
        return (left.favorite_color == right.favorite_color) && (left.num_toes == right.num_toes) &&
               (left.num_teeth == right.num_teeth);
    }

    friend std::ostream &operator<<(std::ostream &s, const O3 &o3) {
        s << "O3(" << o3.favorite_color << ", " << o3.num_toes << ", " << o3.num_teeth << ')';
        return s;
    }
};

TEST(Collate, Structs) {
    auto df1 = DataFrame<std::string, O1>{
        {"ali",                                        "john"                                      },
        {O1{.favorite_color = "green", .num_toes = 6}, O1{.favorite_color = "blue", .num_toes = 10}}
    };

    auto df2 = DataFrame<std::string, O2>{
        {"ali",               "john"             },
        {O2{.num_teeth = 18}, O2{.num_teeth = 32}}
    };

    auto g = df1.collate(df2, [](const O1 &left, const O2 &right) { return O3(left, right); }).materialize();

    EXPECT_EQ(*g.tags, *df1.tags);
    EXPECT_EQ(g[0].v, O3(O1{"green", 6}, O2{18}));
    EXPECT_EQ(g[1].v, O3(O1{"blue", 10}, O2{32}));
}

TEST(Columnar, Simple) {
    std::vector<std::string> tags{"ali", "john"};

    struct Columnar {
        DataFrame<std::string, std::string> favorite_color;
        DataFrame<std::string, int> num_toes;
        DataFrame<std::string, int> num_teeth;
    } df{
        {tags, {"green", "blue"}},
        {tags, {6, 10}          },
        {tags, {18, 32}         }
    };

    auto toes_per_tooth =
        df.num_toes.collate(df.num_teeth, [](int num_toes, int num_teeth) { return float(num_toes) / num_teeth; })
            .materialize();
    EXPECT_EQ(toes_per_tooth.size(), 2);
    EXPECT_EQ(toes_per_tooth[0].v, 6.f / 18);
    EXPECT_EQ(toes_per_tooth[1].v, 10.f / 32);
}

TEST(RangeTags, implicit_tags) {
    auto df_implicit = DataFrame<RangeTag, int>({-1, -2, -3, -4, -5});
    auto df_explicit = DataFrame<RangeTag, int>({5}, {-1, -2, -3, -4, -5});

    EXPECT_NE(df_implicit.tags, df_explicit.tags);
    EXPECT_EQ(df_implicit.tags->size(), df_explicit.tags->size());
}

TEST(RangeTags, advance_to_tag) {
    auto df = DataFrame<RangeTag, int>({-1, -2, -3, -4, -5});
    auto edf = to_expr(df);

    edf.advance_to_tag(3);

    auto [t, v] = df[edf.i];
    EXPECT_EQ(t, 3);
    EXPECT_EQ(v, -4);
}

TEST(RangeTags, advance_to_tag_Missing) {
    auto df = DataFrame<RangeTag, int>({-1, -2, -3, -4, -5});
    auto edf = to_expr(df);

    edf.advance_to_tag(20);

    EXPECT_TRUE(edf.end());
}

TEST(RangeTags, Indexing) {
    auto df = DataFrame<RangeTag, int>({-1, -2, -3, -4, -5});
    auto i = DataFrame<size_t, NoValue>({2, 3}, {});
    auto c = *df[i];

    EXPECT_EQ(*c.tags, (std::vector<size_t>{2, 3}));
    EXPECT_EQ(*c.values, (std::vector<int>{-3, -4}));
}

TEST(Concat, Interleaved_No_Overlap_Finish_With_1) {
    auto df1 = DataFrame<int, float>({1, 4}, {10., 40.});
    auto df2 = DataFrame<int, float>({2, 3}, {20., 30.});

    auto g = df1.concatenate(df2).materialize();

    EXPECT_EQ(g.size(), df1.size() + df2.size());

    EXPECT_EQ(*g.tags, (std::vector<int>{1, 2, 3, 4}));
    EXPECT_EQ(*g.values, (std::vector<float>{10., 20., 30., 40.}));
}

TEST(Concat, Interleaved_No_Overlap_Finish_With_2) {
    auto df1 = DataFrame<int, float>({1, 3}, {10., 30.});
    auto df2 = DataFrame<int, float>({2, 4}, {20., 40.});

    auto g = df1.concatenate(df2).materialize();

    EXPECT_EQ(g.size(), df1.size() + df2.size());

    EXPECT_EQ(*g.tags, (std::vector<int>{1, 2, 3, 4}));
    EXPECT_EQ(*g.values, (std::vector<float>{10., 20., 30., 40.}));
}

TEST(Concat, Interleaved_No_Overlap_Start_With_2) {
    auto df1 = DataFrame<int, float>({2, 3}, {20., 30.});
    auto df2 = DataFrame<int, float>({1, 4}, {10., 40.});

    auto g = df1.concatenate(df2).materialize();

    EXPECT_EQ(g.size(), df1.size() + df2.size());

    EXPECT_EQ(*g.tags, (std::vector<int>{1, 2, 3, 4}));
    EXPECT_EQ(*g.values, (std::vector<float>{10., 20., 30., 40.}));
}

TEST(Concat, Interleaved_With_Overlap) {
    auto df1 = DataFrame<int, float>({1, 2, 3}, {10., 20., 30.});
    auto df2 = DataFrame<int, float>({2, 4}, {21., 40.});

    auto g = df1.concatenate(df2).materialize();

    EXPECT_EQ(g.size(), df1.size() + df2.size());

    EXPECT_EQ(*g.tags, (std::vector<int>{1, 2, 2, 3, 4}));
    EXPECT_EQ(*g.values, (std::vector<float>{10., 21., 20., 30., 40.}));
}

TEST(Concat, concate_and_sum) {
    auto df1 = DataFrame<int, float>({1, 2, 3}, {10., 20., 30.});
    auto df2 = DataFrame<int, float>({2, 4}, {21., 40.});

    auto g = df1.concatenate(df2).reduce_sum().materialize();

    EXPECT_EQ(g.size(), 4);

    EXPECT_EQ(*g.tags, (std::vector<int>{1, 2, 3, 4}));
    EXPECT_EQ(*g.values, (std::vector<float>{10., 41., 30., 40.}));
}

TEST(Materialize, splat) {
    auto df1 = DataFrame<int, float>({1, 2, 3}, {10., 20., 30.});
    auto df2 = DataFrame<int, float>({2, 4}, {21., 40.});

    auto g = *df1.concatenate(df2).reduce_sum();

    EXPECT_EQ(g.size(), 4);

    EXPECT_EQ(*g.tags, (std::vector<int>{1, 2, 3, 4}));
    EXPECT_EQ(*g.values, (std::vector<float>{10., 41., 30., 40.}));
}
