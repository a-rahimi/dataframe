/*
Compile this demo with

   clang++ -Wall -std=c++2b  test_dataframe2.cpp  -lgtest_main -lgtest
*/
#include <gtest/gtest.h>

#include <string>

#include "dataframe2.h"

TEST(Expr_DataFrame, copy) {
    auto df = DataFrame<int, float>({1, 2, 3, 4}, {10., 20., 30., 40.});
    auto df_copy = df;

    EXPECT_EQ(df.tags, df_copy.tags);
    EXPECT_EQ(df.values, df_copy.values);
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
    EXPECT_EQ((*df.tags)[edf.i], 2);
    EXPECT_EQ((*df.values)[edf.i], 20);
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

    auto df = materialize(edf);

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

    auto g = materialize(df[i]);

    EXPECT_EQ(g.size(), 2);
    EXPECT_EQ(*g.tags, (std::vector<int>{2, 3}));
    EXPECT_EQ(*g.values, (std::vector<float>{20., 30.}));
}

TEST(Index, NoValues) {
    auto df = DataFrame<int, float>({1, 2, 3, 4}, {10., 20., 30., 40.});

    auto i = DataFrame<int, NoValue>({2, 3}, {});

    auto g = materialize(df[i]);

    EXPECT_EQ(g.size(), 2);

    EXPECT_EQ(*g.tags, (std::vector<int>{2, 3}));
    EXPECT_EQ(*g.values, (std::vector<float>{20., 30.}));
}

TEST(Reduce, sum) {
    auto df = DataFrame<int, float>({1, 2, 2, 3}, {10., 20., 100., 30.});

    auto g = materialize(Reduce::sum(df));

    EXPECT_EQ(*g.tags, (std::vector<int>{1, 2, 3}));
    EXPECT_EQ(*g.values, (std::vector<float>{10., 120., 30.}));
}

TEST(Reduce, max) {
    auto df = DataFrame<int, float>{
        {1,   2,   2,    3  },
        {10., 20., 100., 30.}
    };

    auto g = materialize(Reduce::max(df));

    EXPECT_EQ(*g.tags, (std::vector<int>{1, 2, 3}));
    EXPECT_EQ(*g.values, (std::vector<float>{10., 100., 30.}));
}

TEST(Reduce, max_manual) {
    auto df = DataFrame<int, float>({1, 2, 2, 3}, {10., 20., 100., 30.});

    auto g = materialize(Reduce::reduce(df, [](float a, float b) { return a > b ? a : b; }));

    EXPECT_EQ(*g.tags, (std::vector<int>{1, 2, 3}));
    EXPECT_EQ(*g.values, (std::vector<float>{10., 100., 30.}));
}

TEST(Join, SimplePair) {
    auto df1 = DataFrame<int, float>({1, 2, 3}, {10., 20., 30.});
    auto df2 = DataFrame<int, float>({1, 2, 3}, {-11., -22., -33.});

    auto g = materialize(Join::collate(df1, df2, [](float v1, float v2) { return std::pair(v1, v2); }));

    EXPECT_EQ(g.size(), 3);
    EXPECT_EQ(*g.tags, (std::vector<int>{1, 2, 3}));
    EXPECT_EQ((*g.values)[0], std::pair(10.f, -11.f));
    EXPECT_EQ((*g.values)[1], std::pair(20.f, -22.f));
    EXPECT_EQ((*g.values)[2], std::pair(30.f, -33.f));
}

TEST(Join, Reduced) {
    auto df1 = DataFrame<int, float>({0, 0, 1}, {10., 20., 30.});
    auto df2 = DataFrame<int, float>({0, 1, 1}, {-11., -22., -33.});

    auto g = materialize(Join::sum(Reduce::sum(df1), Reduce::sum(df2)));

    EXPECT_EQ(g.size(), 2);
    EXPECT_EQ(*g.tags, (std::vector<int>{0, 1}));
    EXPECT_EQ(*g.values, (std::vector<float>{19., -25.}));
}

TEST(Join, ReducedLeftDuplicates) {
    auto df1 = DataFrame<int, float>({1, 2, 2, 3}, {10., 20., 100., 30.});
    auto df2 = DataFrame<int, float>({1, 2, 3}, {-11., -22., -33.});

    auto g = materialize(Join::sum(Reduce::sum(df1), Reduce::sum(df2)));

    EXPECT_EQ(g.size(), 3);

    EXPECT_EQ(*g.tags, (std::vector<int>{1, 2, 3}));
    EXPECT_EQ(*g.values, (std::vector<float>{-1., 98., -3.}));
}

TEST(Join, Strings) {
    auto df1 = DataFrame<std::string, float>({"ali", "john"}, {1., 2.});
    auto df2 = DataFrame<std::string, float>({"ali", "john"}, {10., 20.});

    auto g = materialize(Join::sum(df1, df2));

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
};

bool operator==(const O3 &left, const O3 &right) {
    return (left.favorite_color == right.favorite_color) && (left.num_toes == right.num_toes) &&
           (left.num_teeth == right.num_teeth);
}

std::ostream &operator<<(std::ostream &s, const O3 &o3) {
    s << "O3(" << o3.favorite_color << ", " << o3.num_toes << ", " << o3.num_teeth << ')';
    return s;
}

TEST(Join, Structs) {
    auto df1 = DataFrame<std::string, O1>{
        {"ali",                                        "john"                                      },
        {O1{.favorite_color = "green", .num_toes = 6}, O1{.favorite_color = "blue", .num_toes = 10}}
    };

    auto df2 = DataFrame<std::string, O2>{
        {"ali",               "john"             },
        {O2{.num_teeth = 18}, O2{.num_teeth = 32}}
    };

    auto g = materialize(Join::collate(df1, df2, [](const O1 &left, const O2 &right) { return O3(left, right); }));

    EXPECT_EQ(*g.tags, *df1.tags);
    EXPECT_EQ((*g.values)[0], O3(O1{"green", 6}, O2{18}));
    EXPECT_EQ((*g.values)[1], O3(O1{"blue", 10}, O2{32}));
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

    auto toes_per_tooth = materialize(Join::collate(
        df.num_toes, df.num_teeth, [](int num_toes, int num_teeth) { return float(num_toes) / num_teeth; }));
    EXPECT_EQ(toes_per_tooth.size(), 2);
    EXPECT_EQ((*toes_per_tooth.values)[0], 6.f / 18);
    EXPECT_EQ((*toes_per_tooth.values)[1], 10.f / 32);
}

TEST(RangeTags, advance_to_tag) {
    auto df = DataFrame<RangeTag, int>{
        {5},
        {-1, -2, -3, -4, -5}
    };
    auto edf = to_expr(df);

    edf.advance_to_tag(3);

    EXPECT_EQ((*df.tags)[edf.i], 3);
    EXPECT_EQ((*df.values)[edf.i], -4);
}

TEST(RangeTags, advance_to_tag_Missing) {
    auto df = DataFrame<RangeTag, int>({5}, {-1, -2, -3, -4, -5});
    auto edf = to_expr(df);

    edf.advance_to_tag(20);

    EXPECT_TRUE(edf.end());
}

TEST(RangeTags, Indexing) {
    auto df = DataFrame<RangeTag, int>({5}, {-1, -2, -3, -4, -5});
    auto i = DataFrame<size_t, NoValue>({2, 3}, {});
    auto c = materialize(df[i]);

    EXPECT_EQ(*c.tags, (std::vector<size_t>{2, 3}));
    EXPECT_EQ(*c.values, (std::vector<int>{-3, -4}));
}

TEST(Concat, Interleaved_No_Overlap_Finish_With_1) {
    auto df1 = DataFrame<int, float>({1, 4}, {10., 40.});
    auto df2 = DataFrame<int, float>({2, 3}, {20., 30.});

    auto g = materialize(concatenate(df1, df2));

    EXPECT_EQ(g.size(), df1.size() + df2.size());

    EXPECT_EQ(*g.tags, (std::vector<int>{1, 2, 3, 4}));
    EXPECT_EQ(*g.values, (std::vector<float>{10., 20., 30., 40.}));
}

TEST(Concat, Interleaved_No_Overlap_Finish_With_2) {
    auto df1 = DataFrame<int, float>({1, 3}, {10., 30.});
    auto df2 = DataFrame<int, float>({2, 4}, {20., 40.});

    auto g = materialize(concatenate(df1, df2));

    EXPECT_EQ(g.size(), df1.size() + df2.size());

    EXPECT_EQ(*g.tags, (std::vector<int>{1, 2, 3, 4}));
    EXPECT_EQ(*g.values, (std::vector<float>{10., 20., 30., 40.}));
}

TEST(Concat, Interleaved_No_Overlap_Start_With_2) {
    auto df1 = DataFrame<int, float>({2, 3}, {20., 30.});
    auto df2 = DataFrame<int, float>({1, 4}, {10., 40.});

    auto g = materialize(concatenate(df1, df2));

    EXPECT_EQ(g.size(), df1.size() + df2.size());

    EXPECT_EQ(*g.tags, (std::vector<int>{1, 2, 3, 4}));
    EXPECT_EQ(*g.values, (std::vector<float>{10., 20., 30., 40.}));
}

TEST(Concat, Interleaved_With_Overlap) {
    auto df1 = DataFrame<int, float>({1, 2, 3}, {10., 20., 30.});
    auto df2 = DataFrame<int, float>({2, 4}, {21., 40.});

    auto g = materialize(concatenate(df1, df2));

    EXPECT_EQ(g.size(), df1.size() + df2.size());

    EXPECT_EQ(*g.tags, (std::vector<int>{1, 2, 2, 3, 4}));
    EXPECT_EQ(*g.values, (std::vector<float>{10., 21., 20., 30., 40.}));
}

TEST(Concat, concate_and_sum) {
    auto df1 = DataFrame<int, float>({1, 2, 3}, {10., 20., 30.});
    auto df2 = DataFrame<int, float>({2, 4}, {21., 40.});

    auto g = materialize(Reduce::sum(concatenate(df1, df2)));

    EXPECT_EQ(g.size(), 4);

    EXPECT_EQ(*g.tags, (std::vector<int>{1, 2, 3, 4}));
    EXPECT_EQ(*g.values, (std::vector<float>{10., 41., 30., 40.}));
}
