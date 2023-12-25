/*
Compile this demo with

   clang++ -Wall -std=c++2b  test_dataframe2.cpp  -lgtest_main -lgtest
*/

#include <gtest/gtest.h>

#include <string>

#include "dataframe.h"

TEST(DataFrame, copy_by_reference) {
    auto df = DataFrame<int, float>({1, 2, 3, 4}, {10., 20., 30., 40.});
    auto df_copy = df;

    EXPECT_EQ(df.tags, df_copy.tags);
    EXPECT_EQ(df.values, df_copy.values);
    EXPECT_EQ(*df.tags, *df_copy.tags);
    EXPECT_EQ(*df.values, *df_copy.values);
}

TEST(DataFrame, to_dataframe_makes_reference) {
    auto df = DataFrame<int, float>({1, 2, 3, 4}, {10., 20., 30., 40.});
    auto df_copy = df.to_dataframe();

    EXPECT_EQ(df.tags, df_copy.tags);
    EXPECT_EQ(df.values, df_copy.values);
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

TEST(DataFrame, constant_value) {
    const DataFrame<int, ConstantValue<std::string>> df({1, 2, 3, 4}, {"hello"});

    EXPECT_EQ(df[1].v, "hello");
}

TEST(Expr_Dataframe, to_dataframe_materializes) {
    auto df = DataFrame<int, float>({1, 2, 3, 4}, {10., 20., 30., 40.});
    auto df_copy = df.to_expr().to_dataframe();

    EXPECT_NE(df.tags, df_copy.tags);
    EXPECT_NE(df.values, df_copy.values);
    EXPECT_EQ(*df.tags, *df_copy.tags);
    EXPECT_EQ(*df.values, *df_copy.values);
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

TEST(Expr_DataFrame, find_tag_missing_in_the_middle) {
    auto df = DataFrame<int, float>({1, 3, 4}, {10., 30., 40.});
    auto edf = to_expr(df);

    edf.advance_to_tag(2);

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

TEST(Index, ConstantValue) {
    auto df = DataFrame<int, float>({1, 2, 3, 4}, {10., 20., 30., 40.});

    auto i = DataFrame<int, ConstantValue<int>>({2, 3}, {0});

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

TEST(Reduce, RangeTag) {
    auto df = DataFrame<RangeTag, float>({3}, {10., 20., 30.});

    auto g = df.reduce_sum().materialize();

    EXPECT_EQ(*g.tags, (std::vector<size_t>{0, 1, 2}));
    EXPECT_EQ(*g.values, (std::vector<float>{10., 20., 30.}));
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

    auto g = df.apply([](float v) { return v / 2; }).reduce_moments().materialize();

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

    auto g = df.apply([](float v) { return v / 2; }).materialize();

    EXPECT_EQ(*g.tags, (std::vector<int>{1, 2, 2, 3}));
    EXPECT_EQ(*g.values, (std::vector<float>{5., 10., 50., 15.}));
}

TEST(Apply, output_types) {
    auto df = DataFrame<std::string, float>({"hi", "ho", "hello"}, {10., 20., 30.});

    auto g = df.apply([](const std::string &t, float v) { return v / 2; }).materialize();

    EXPECT_EQ(*g.tags, *df.tags);
    EXPECT_EQ(*g.values, (std::vector<float>{5., 10., 15.}));
}

TEST(Apply, consecutive_tags) {
    auto df = DataFrame<size_t, float>({0, 1, 2}, {10., 20., 30.});

    auto g = df.apply([](float v) { return v / 2; }).materialize();

    EXPECT_EQ(*g.tags, (std::vector<size_t>{0, 1, 2}));
    EXPECT_EQ(*g.values, (std::vector<float>{5., 10., 15.}));
}

TEST(Apply, RangeTag) {
    auto df = DataFrame<RangeTag, float>({3}, {10., 20., 30.});

    auto g = df.apply([](float v) { return v / 2; }).materialize();

    EXPECT_EQ(*g.tags, (std::vector<size_t>{0, 1, 2}));
    EXPECT_EQ(*g.values, (std::vector<float>{5., 10., 15.}));
}

TEST(Apply, find_tag) {
    auto df = DataFrame<int, float>({1, 2, 2, 3}, {10., 20., 100., 30.});

    auto expr = df.apply([](float v) { return v / 2; });
    expr.advance_to_tag(2);
    auto g = expr.materialize();

    EXPECT_EQ(*g.tags, (std::vector<int>{2, 2, 3}));
    EXPECT_EQ(*g.values, (std::vector<float>{10., 50., 15.}));
}

TEST(Apply, find_tag_last) {
    auto df = DataFrame<int, float>({1, 2, 2, 3}, {10., 20., 100., 30.});

    auto expr = df.apply([](float v) { return v / 2; });
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

    auto g = df1.reduce_sum().collate(df2.reduce_sum(), std::plus<>()).materialize();

    EXPECT_EQ(g.size(), 2);
    EXPECT_EQ(*g.tags, (std::vector<int>{0, 1}));
    EXPECT_EQ(*g.values, (std::vector<float>{19., -25.}));
}

TEST(Collate, ReducedLeftDuplicates) {
    auto df1 = DataFrame<int, float>({1, 2, 2, 3}, {10., 20., 100., 30.});
    auto df2 = DataFrame<int, float>({1, 2, 3}, {-11., -22., -33.});

    auto g = df1.reduce_sum().collate(df2.reduce_sum(), std::plus<>()).materialize();

    EXPECT_EQ(g.size(), 3);

    EXPECT_EQ(*g.tags, (std::vector<int>{1, 2, 3}));
    EXPECT_EQ(*g.values, (std::vector<float>{-1., 98., -3.}));
}

TEST(Collate, df1_has_missing_values) {
    auto df1 = DataFrame<int, float>({1, 3}, {10., 30.});
    auto df2 = DataFrame<int, float>({1, 2, 3}, {-11., -22., -33.});

    auto g = df1.collate(df2, std::plus<>()).materialize();

    EXPECT_EQ(g.size(), 2);

    EXPECT_EQ(*g.tags, (std::vector<int>{1, 3}));
    EXPECT_EQ(*g.values, (std::vector<float>{-1., -3.}));
}

TEST(Collate, Strings) {
    auto df1 = DataFrame<std::string, float>({"ali", "john"}, {1., 2.});
    auto df2 = DataFrame<std::string, float>({"ali", "john"}, {10., 20.});

    auto g = df1.collate(df2, std::plus<>()).materialize();

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

TEST(RangeTags, expression) {
    auto edf = DataFrame<RangeTag, int>({5}, {-1, -2, -3, -4, -5}).to_expr();

    EXPECT_EQ(edf.i, 0);
    EXPECT_EQ(edf.tag, 0);
    EXPECT_EQ(edf.value, -1);

    edf.next();
    EXPECT_EQ(edf.i, 1);
    EXPECT_EQ(edf.tag, 1);
    EXPECT_EQ(edf.value, -2);
}

TEST(RangeTags, expression_materialized) {
    auto edf = DataFrame<RangeTag, int>({5}, {-1, -2, -3, -4, -5}).to_expr();
    auto df = edf.materialize();

    ASSERT_EQ(*df.tags, (std::vector<size_t>{0, 1, 2, 3, 4}));
}

TEST(RangeTags, advance_to_tag) {
    auto df = DataFrame<RangeTag, int>({5}, {-1, -2, -3, -4, -5});
    EXPECT_EQ(df.tags->size(), 5);

    auto edf = to_expr(df);

    edf.advance_to_tag(3);
    EXPECT_EQ(edf.i, 3);
    EXPECT_EQ(edf.tag, 3);
    EXPECT_EQ(edf.value, -4);

    auto [t, v] = df[edf.i];
    EXPECT_EQ(t, 3);
    EXPECT_EQ(v, -4);
}

TEST(RangeTags, advance_to_tag_Missing) {
    auto df = DataFrame<RangeTag, int>({5}, {-1, -2, -3, -4, -5});
    auto edf = to_expr(df);

    edf.advance_to_tag(20);

    EXPECT_TRUE(edf.end());
}

TEST(RangeTags, Indexing) {
    auto df = DataFrame<RangeTag, int>({5}, {-1, -2, -3, -4, -5});
    auto i = DataFrame<size_t, ConstantValue<int>>({2, 3}, {0});
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

struct Match {
    std::string player1;
    std::string player2;
    int score_player1;
    int score_player2;
};

DataFrame<RangeTag, Match> matches{
    {4},
    {
     {"ali", "john", 10, 5},
     {"ali", "john", 6, 8},
     {"ali", "misha", 4, 6},
     {"misha", "john", 5, 7},
     }
};

TEST(Concatenate, concatenate_apply) {
    auto players_1 = matches.apply([](const Match &m) { return m.player1; });
    auto players_2 = matches.apply([](const Match &m) { return m.player2; });
    auto players = *players_1.concatenate(players_2);
    ASSERT_EQ(*players.tags, (std::vector<size_t>{0, 0, 1, 1, 2, 2, 3, 3}));
    ASSERT_EQ(*players.values,
              (std::vector<std::string>{"john", "ali", "john", "ali", "misha", "ali", "john", "misha"}));
}

TEST(Concatenate, concatenate_apply_interim_materialization) {
    auto players_1 = *matches.apply([](const Match &m) { return m.player1; });
    ASSERT_EQ(*players_1.tags, (std::vector<size_t>{0, 1, 2, 3}));
    ASSERT_EQ(*players_1.values, (std::vector<std::string>{"ali", "ali", "ali", "misha"}));

    auto players_2 = *matches.apply([](const Match &m) { return m.player2; });
    ASSERT_EQ(*players_2.tags, (std::vector<size_t>{0, 1, 2, 3}));
    ASSERT_EQ(*players_2.values, (std::vector<std::string>{"john", "john", "misha", "john"}));

    auto players = *players_1.concatenate(players_2);
    ASSERT_EQ(*players.tags, (std::vector<size_t>{0, 0, 1, 1, 2, 2, 3, 3}));
    ASSERT_EQ(*players.values,
              (std::vector<std::string>{"john", "ali", "john", "ali", "misha", "ali", "john", "misha"}));
}

TEST(Materialize, splat) {
    auto df1 = DataFrame<int, float>({1, 2, 3}, {10., 20., 30.});
    auto df2 = DataFrame<int, float>({2, 4}, {21., 40.});

    auto g = *df1.concatenate(df2).reduce_sum();

    EXPECT_EQ(g.size(), 4);

    EXPECT_EQ(*g.tags, (std::vector<int>{1, 2, 3, 4}));
    EXPECT_EQ(*g.values, (std::vector<float>{10., 41., 30., 40.}));
}

TEST(Retag, floats) {
    auto df = DataFrame<std::string, float>({"hi", "ho", "hello"}, {20., 10., 30.});
    auto g = *df.retag([](const std::string &t, float v) { return -v; });

    EXPECT_EQ(g.size(), 3);
    EXPECT_EQ(*g.tags, (std::vector<float>{-30., -20., -10.}));
    EXPECT_EQ(*g.values, (std::vector<float>{30., 20., 10.}));
}

TEST(Retag, floats_with_repeats) {
    auto df = DataFrame<std::string, float>({"hi", "ho", "ho", "hello"}, {20., 20., 10., 30.});
    auto g = *df.retag([](const std::string &t, float v) { return -v; });

    EXPECT_EQ(g.size(), 4);
    EXPECT_EQ(*g.tags, (std::vector<float>{-30., -20., -20., -10.}));
    EXPECT_EQ(*g.values, (std::vector<float>{30., 20., 20., 10.}));
}

TEST(Retag, floats_with_repeats_2) {
    auto df = DataFrame<std::string, float>({"hi", "ho", "ho", "hello"}, {20., 10., 10., 30.});
    auto g = *df.retag([](const std::string &t, float v) { return -v; });

    EXPECT_EQ(g.size(), 4);
    EXPECT_EQ(*g.tags, (std::vector<float>{-30., -20., -10., -10.}));
    EXPECT_EQ(*g.values, (std::vector<float>{30., 20., 10., 10.}));
}

TEST(Retag, materialized_dataframes) {
    auto df_values = DataFrame<std::string, float>({"hi1", "ho1", "ho1", "hello1"}, {20., 10., 10., 30.});
    auto df_tags = DataFrame<std::string, float>({"hi2", "ho2", "ho2", "hello2"}, {-20., -10., -10., -30.});
    auto g = *df_values.retag(df_tags);

    EXPECT_EQ(g.size(), 4);
    EXPECT_EQ(*g.tags, (std::vector<float>{-30., -20., -10., -10.}));
    EXPECT_EQ(*g.values, (std::vector<float>{30., 20., 10., 10.}));
}

TEST(Argsort, strings) {
    std::vector<size_t> indices;
    argsort(std::vector<std::string>{"Zaa", "Aaa", "Bbb"}, indices);
    EXPECT_EQ(indices, (std::vector<size_t>{1, 2, 0}));
}

TEST(MatchDemo, demo) {
    auto num_games_won = constant(matches.size(), 1)
                             .retag(matches.apply([](const Match &m) {
                                 return m.score_player1 > m.score_player2 ? m.player1 : m.player2;
                             }))
                             .reduce_sum();

    auto num_games_played = constant(2 * matches.size(), 1)
                                .retag(matches.apply([](const Match &m) { return m.player1; })
                                           .concatenate(matches.apply([](const Match &m) { return m.player2; })))
                                .reduce_sum();

    auto win_rate = *num_games_won.collate(num_games_played, [](int wins, int games) { return float(wins) / games; });

    ASSERT_EQ(*win_rate.tags, (std::vector<std::string>{"ali", "john", "misha"}));
    ASSERT_EQ(*win_rate.values, (std::vector<float>{1. / 3, 2. / 3, 0.5}));
}

template <typename Expr>
auto count_values(Expr expr) {
    auto df = expr.to_dataframe();
    return constant(df.size(), 1).retag(df).reduce_sum();
}

TEST(MatchDemo, demo2) {
    auto num_games_won = count_values(
        matches.apply([](const Match &m) { return m.score_player1 > m.score_player2 ? m.player1 : m.player2; }));

    auto num_games_played = count_values(
        matches.apply([](const Match &m) { return m.player1; }).concatenate(matches.apply([](const Match &m) {
            return m.player2;
        })));

    auto win_rate = *num_games_won.collate(num_games_played, [](int wins, int games) { return float(wins) / games; });

    ASSERT_EQ(*win_rate.tags, (std::vector<std::string>{"ali", "john", "misha"}));
    ASSERT_EQ(*win_rate.values, (std::vector<float>{1. / 3, 2. / 3, 0.5}));
}

TEST(MatchDemo, demo_native) {
    // Implement MatchDemo without without dataframes.

    std::map<std::string, int> num_games_won;
    for (const Match &m : *matches.values) {
        const std::string &winner = m.score_player1 > m.score_player2 ? m.player1 : m.player2;
        num_games_won[winner] += 1;
    }

    std::map<std::string, int> num_games_played;
    for (const Match &m : *matches.values) {
        num_games_played[m.player1] += 1;
        num_games_played[m.player2] += 1;
    }

    std::map<std::string, float> win_rate;
    for (auto [player, won] : num_games_won) {
        win_rate[player] = float(won) / num_games_played[player];
        // std::cout << player << '\t' << win_rate[player] << '\n';
    }
}