# Introduction

Dataframes are datastructures that facilitate statistical operations on
datasets. They borrow ideas from data query languages like SQL , but they're
data structures intended to be used inside a host programming language.  Since
their introduction to the S programming language in 1990, dataframes have been
enhanced and cleaned up in many ways. For example, the R programming language
introduced [dplyr](http://dplyr.tidyverse.org), which expands the semantics of
data frames, [Pandas](https://pandas.pydata.org) implements dataframes for
Python, and [Polars](https://www.pola.rs) implements them in Rust.

Traditionally, dataframes are two-dimensional tables, like tables in relational
database, or like numerical matrices.  Unlike a matrix, a dataframe may contain
values of different types, and unlike traditional relational databases, the
table can be stored in columnar format (where each column is represented as a
vector of homogenous type), or in row format, where each row is a heterogenous
record.  Typical operations on a dataframe produce new dataframes by selecting
subsets of rows or columns of these tables (similar to SQL's "SELECT"), grouping
rows or columns and computing summary statistics on these groups (similar to
SQL's "GROUP BY"), or joining multiple dataframes by combining their rows
whenever some of their match (similar to SQL's "JOIN").

This package implements a new, simplified dataframe in C++.

# How this package differs from other dataframe packages

 Dataframes in R and Python are helpful because they implement in native code
 operations that would otherwise be slow in interpreted code. For example,
 applying a user-supplied Python function to every row of a Pandas dataframe is
 slow, so Pandas implements a huge variety of operations in native code to avoid
 calling Python callbacks.  In C++, this is not an issue.  Applying a
 user-supplied C++ lambda to every row of a dataframe is not any slower than
 applying a built-in method. As such, a C++ dataframe library can be much
 simpler. Another consideration is that C++ already provides excellent
 facilities to manipulated structured data through structs, whereas in
 interpreted languages, structured data are represented through dynamic data
 structures that are slower to manipulated.  Here were the overarching desgin
 goals of this package:

* Do as much work at compile-time as possible. The dataframes are statically
  typed so there is no overhead to represent schemas, or interpret datatypes at
  run-time. Eventually, I'd like the query planning to happen at compile time as
  well, but I haven't built complex-enough queries to warrant a query yet, let
  alone one that runs at compile time.

* Portable to new compute backends. Most of the operations reduce to a call to a
  function called "merge", which does most of the work. When porting the package to a
  new compute substrate, like a distributed system or a GPU, most of
  the code could be left intact. One would just port the merge function.

* Simple semantics. The semantics of this package are slightly more complicated
  than Polars, which does not rely on a notion of a dataframe index, but
  significantly simpler than those of dplyr or Pandas.

Implementing a new dataframe library or porting one to a new compute engine (for
example a distributed system, or a GPU) is a lot of work because  the algebra on
a traditional dataframe is surprisingly complicated.  For example [this
analysis](https://escholarship.org/uc/item/9x5608wr) derives a dataframe API
with 14 entry points. The API for R's dplyr dataframe package ostensibly has
only four entry points (mutate, select, filter, summarize, arrange), but each
entry point provides a plethora of options  that radically modify their
behavior. Some of these entry points even provide a mini-language of their own.

The semantics for the dataframes implemented in this package are simple to
define an implement. Almost all the operations rely on one workhorse function
called "merge()". The various operations affect the behavior of `merge` by
passing different reduction functions. A "merge" genealizes a traditional join
between two dataframes by allowing the caller to supply callbacks that change
how rows that match between the two dataframes are combined, and how to combine
multiple rows that match.  Inner joins, outer joins, selecting rows, and
grouping operations are all implemented by calling merge() with slightly
different callbacks.

Unlike traditional dataframes, this package does not take a position on row-wise
vs columnar storage. Its dataframe are 1D vectors.  When the elements of these
vectors are records, the dataframes behave like row-wise dataframes. When
several vectors of scalars are combined in a C++ struct, they behave like a
columnar store. See the section below for details.

This package borrows and modifies the notion of an index from SQL and Pandas.
These indices are called "tags" here, and they are involved in every operation
the package implements, including join, groupby, and indexing. Each entry of a
dataframe is tagged with a value. In the simplest case, these tags are
consecutive numbers so that the tag just represents the row id of the element.
This case compiles down to the fastest path, where operations on dataframes are
as fast as operations with fast linear algebra packages.  But more generally,
tags can more be any type and take on any value so long as all entries of the
dataframe have the same tag type. Joining two dataframes amounts to combining
entries of two dataframes that have the same tag value. The groupby operation
reduces all entries of a dataframe that have the same tag value.  Select entries
of a dataframe happens by collecting all entries that have a given set of tag
values. 

# Data types

A dataframe is tuple of two vectors: a tag vector, and a values vector:

```
DataFrame<Tag, T> = (tags: std::vector<Tag>, values: std::vector<T>)
```

We'll rely on some special subtypes of `std::vector` to speed up certain
operations.  For example, `std::vector<Range>`, represents an arithmetic
sequence of integers and is used to represent  the default tags. This vector
doesn't actually store the values of these sequences. Instead, it computes them
as needed. The merge operation follows an accelerated path in these cases.

# Dataframe Semantics

## General merge

The main workhorse of these dataframes is the merge function:

```
left: DataFrame<Tag, Ta>
right: DataFrame<Tag, Tb>
merged: DataFrame<Tag, Tc>

merged = merge(left, right, combine_left_right_op, accumulate_op)
```

The functions `combine_left_right_op`  has signature

(tag_left: Union[Tag, NoTag], tag_right: Union[Tag, NoTag], v_left: TLeft,  v_right: TRight) -> (Tag, Tout)

It is called on every tag `t` in union(a,b), so that 

```
(taga=t or taga:NoTag) and (tagb=t or tagb:NoTag).
```

It returns the result of the rows that have matching tags.

The function `accumulate` has signature

```
(tag_accumulation: Tag, v_accumulation: Tacc,  value_left: TLeft) -> Tag, Tout
```

It is used to merge rows of the left dataframe that have the same value after
they've been merged with the right dataframe.

If the tag returned by `combine_left_right_op` or `accumulate` have type NoTag,
the result is not saved in the output dataframe.

All operations below are implemented in terms of merge by passing different
functions for `combine_left_right_op` and `accumulate`.

## Indexing 

DataFrames can be sliced and indexed similarly to how matrices can be indexed in
numerical packages like [Eigen](https://eigen.tuxfamily.org/) or
[numpy](http://numpy.org):

```
a: DataFrame<Tag, Ta>
b: DataFrame<Tag, Tb>
c: DataFrame[Tag, Ta]

c = a[b] 
```

The above computes a dataframe that consists of rows of `a` that have the same
tag as `b`.  Duplicated tags in `b` result in duplicated tags in `c`. Duplicated
tags in `a` result in undefined behavior (in reality, they get summed, but this
will likely change in the future).

Example:
```
// A dataframe with tags 1...4, and values 10...40 in steps of 10.
auto df = DataFrame<int, float>{.tags={1, 2, 3, 4}, .values={10., 20., 30., 40.}};

// A dataframe with tags 2, 3, and values that will be ignored when indexing.
auto i = DataFrame<int, float>{.tags={2, 3}, .values={-20., -30.}};

// g has two entries, with tags 2 and 3 and values 20. and 30.
auto g = df[i];
```

## Inner Join 

```
a: DataFrame<Tag, Ta>
b: DataFrame<Tag, Tb>
c = a[b] : DataFrame[Tag, Ta]
```

Returns a dataframe consisting of rows of a and b that have the same tag.
Duplicate values can be summed, or returned in pairs, or combined in any other
way specified by the join operation.

Example:
```
auto df1 = DataFrame<int, float>{.tags={1, 2, 3}, .values={10., 20., 30.}};
auto df2 = DataFrame<int, float>{.tags={1, 2, 3}, .values={-11., -22., -33.}};

// g has tags 1,2,3 and values -1, -2, -3.
auto g = Join::sum(df1, df2);

// gp has tags 1,2,3 and values pair(10,-11), pair(20,-22), pair(30,-33).
auto gp = Join::pair(df1, df2)
```

## Outer Join 

```
a: DataFrame<Tag, Ta>
b: DataFrame<Tag, Tb>
c = a[b] : DataFrame[Tag, Ta]
```


## Grouping

The grouping operation is similar to groupby in SQL and traditional dataframes, except that the group ids must be the tags of the dataframe:

```
a: DataFrame<Tag, T>
b: DataFrame<Tag, Tacc>

b = Group::sum(a)
```

This applies the reduction `sum` to all entries of `a` that have the same tag. The
resulting dataframe has one row per unique tag in `a`, and the corresponding entry
is the result of the reduction.

```
auto df = DataFrame<int, float>{.tags={1, 2, 2, 3}, .values={10., 20., 100., 30.}};

// g has tags 1,2,3 and values 10, 120, 30.
auto g = Group::sum(df);
```

# Example of row-wise dataframe storage

The vignette below illustrates row-wise storage. We'll define two dataframes, each of whose elements is a struct:

```
struct O1
{
    std::string favorite_color;
    int num_toes;
};

auto df1 = DataFrame<std::string, O1>{
    {"ali", "john"},
    {O1{.favorite_color = "green", .num_toes = 6}, O1{.favorite_color = "blue", .num_toes = 10}}
};


struct O2
{
    int num_teeth;
};

auto df2 = DataFrame<std::string, O2>{
    {"ali", "john"},
    {O2{.num_teeth = 18}, O2{.num_teeth = 32}}
};
```

We can join these two dataframes on their tags to obtain a third dataframe whose elements are a third kind of struct: 

```
struct O3 : O1, O2
{
    O3(const O1 &o1, const O2 &o2) : O1(o1), O2(o2) {}
};

// g is a dataframe whose elements on of type O3.
auto g = Join::collate(
    df1, df2, [](const O1 &left, const O2 &right) { return O3(left, right); }
);

assert(g.tags == df1.tags);
assert(g.values[0] == O3(O1{"green", 6}, O2{18}));
assert(g.values[1] == O3(O1{"blue", 10}, O2{32}));
```

# Example of columnar dataframe storage

The example below represents the above dataset in a columnar format:

```
auto tags = std::vector<std::string>{"ali", "john"};

struct Columnar
{
    DataFrame<std::string, std::string> favorite_color;
    DataFrame<std::string, int> num_toes;
    DataFrame<std::string, int> num_teeth;
} df {
    {tags, {"green", "blue"}},
    {tags, {6, 10}},
    {tags, {18, 32}}
};
```

Here, each field is a separate dataframe, and the dataframes are collected in a
struct. In this particular example, the tags are replicated across the three
fields because there is no mechanism yet to share tags (an upcoming feature).

Here is a simple operation we can perform on this columnar data structure:

```
// A float-valued dataframe that computes the ratio of teeth to toes for each
// row of the columnar dataframe above.
auto toes_per_tooth = Join::collate(
    df.num_toes,
    df.num_teeth,
    [](int a, int b) { return float(a) / b; }
);
```
