# Introduction


Dataframes are  datastructures that facilitate statistical operations on
datasets. They borrow ideas from to the kinds of operations SQL facilitates for
on-disk data.  Since their introduction to the S programming language in 1990,
dataframes have been enhanced and cleaned up in many ways. For example, the R
programming language introduced dplyr, which expands the semantics of data
frames, Pandas implements dataframes for Python, and Polars implements them in
Rust.

Traditionally, dataframes are two-dimensional tables, like tables in relational
database, or like numerical matrices.  Unlike a matrix, each column of a
dataframe may contain values of different types. Unlike traditional relationan
databases, the table can be stored in columnar format (where each column is
represented as a vector of homogenous type), or in row format, where each row is
a heterogenous record.  Typical operations on a dataframe produce new dataframes
by selecting subsets of rows or columns of these tables (similar to SQL's
"SELECT"), grouping rows or columns and computing summary statistics on these
groups (similar to SQL's "GROUP BY"), or joining multiple dataframes by
combining their rows whenever some of their match (similar to SQL's "JOIN").

Implementing a new dataframe library or porting one to a new compute engine (for
example a distributed system, or a GPU) is a lot of work because  the algebra on
a traditional dataframe is surprisingly complicated.  For example [this
analysis](https://escholarship.org/uc/item/9x5608wr) derives a dataframe API
with 14 entry points. The API for R's dplyr dataframe package ostensibly has
only four entry points (mutate, select, filter, summarize, arrange), but each
entry point provides a plethora of options  that radically modify their
behavior. Some of these entry points even provide a mini-language of their own.

This package implements a new, simplified dataframe in C++. Here were the overarching desgin goals:

* To as much work at comile time as possible. The dataframes are statically typed, so
  there is no overhead to represent schemas, or interpret datatypes at run-time.

* Portable to new compute backends. Most of the operations reduce to a call to a
  function called "merge", which does most of the work. Porting the package to a
  new compute substrate, like a distributed system or a GPU would leave most of
  the code intact, and port just the merge function.

* Simple semantics. The semantics of this package are slightly more complicated
  than Polars, but significantly simpler than those in dplyr or Pandas.

# How this package differs from other dataframe packages

The semantics for these dataframes are simple to define an implement: almost all
the operations rely on one workhorse function called "merge()", and they affect
the behavior of merge by passing different reduction functions. A merge is a
generalized a join operation between two dataframes, allowing the caller to
specify through callbacks how matching rows are combined, and how to combine
multiple rows that match. Inner joins, outer joins, selecting rows, and grouping
operations are all implemented by calling merge() with slightly different
callbacks.

Unlike traditional dataframes, this package does not take a position on row-wise
vs columnar storage. The dataframe are 1D vectors.  When these are vectors of
records, the dataframes behave like row-wise dataframes. When several vectors of
scalars are combined in a C++ struct, they behave like a columnar store.

This package borrows and modifies the notion of an index from SQL and Pandas,
and they are involved in every operation the package implements, including join,
groupby, and indexing. Each entry of a dataframe is tagged with a value. In the
simplest case, these tags could be consecutive numbers so that the tag just
represents the row id of the element.  But the tags of a dataframe can be any
type so long as all entries of the dataframe have the same tag type. Joining two
dataframes boils down to combining entries of two dataframes that have the same
tag value. The groupby operation reduces all entries of a dataframe that have
the same tag value. Select entries of a dataframe happens by collecting all
entries that have a given set of tag values. 

# Data types

A dataframe is tuple of two vectors: and tag vector, and the values vector:

Vector<T> = [v[0], ...], where v[i: int]: T.
DataFrame<Tag,T> = (tags: Vector<Tag>, values: Vector<T>)

We'll rely on some special subtypes of Vector to speed up certain operations. For example,

Range = Vector<int>, which contains arithmetic sequences of integers. We'll use
these as the default tag vector.

# Semantics

## General merge

left: DataFrame<Tag, Ta>
right: DataFrame<Tag, Tg>
c = merge(left, right, combine_left_right_op, accumulate_op): DataFrame<Tag, Top>

The functions `combine_left_right_op`  has signature

(tag_left: Union[Tag, NoTag], tag_right: Union[Tag, NoTag], v_left: TLeft,  v_right: TRight) -> Tag, Tout

It is called on every tag `t` in union(a,b), so that 

   (taga=t or taga:NoTag) and (tagb=t or tagb:NoTag).

It returns the result of the rows that have matching tags.

The function `accumulate` has signature signature

(tag_accumulation: Tag, v_accumulation: Tacc,  value_left: TLeft) -> Tag, Tout

It ise used to merge rows of the left dataframe that have the same value after
they've been merged with the right dataframe.

If the tag returned by `combine_left_right_op` or `accumulate` have type NoTag, the result is not saved in c.

All operations below are implemented in terms of merge by passing different
functions for `combine_left_right_op` and `accumulate`.

## Indexing 

a: DataFrame<Tag, Ta>
b: DataFrame<Tag, Tb>
c = a[b] : DataFrame[Tag, Ta]

Returns a dataframe consisting of rows of a that have the same tag as b.
Duplicated tags in b result in duplicated tags in c. Duplicated tags in a result
in undefined behavior (in reality, they get summed, but this will likely change
in the future).

Example:
```
    // A dataframe with tags 1...4, and values 10...40 in steps of 10.
    auto df = DataFrame<int, float>{{1, 2, 3, 4}, {10., 20., 30., 40.}};

    // A dataframe with tags 2, 3, and values that will be ignored.
    auto i = DataFrame<int, float>{{2, 3}, {-20., -30.}};

    // g has two entries, with tags 2 and 3 and values 20. and 30.
    auto g = df[i];
```

## Inner Join 

a: DataFrame<Tag, Ta>
b: DataFrame<Tag, Tb>
c = a[b] : DataFrame[Tag, Ta]

Returns a dataframe consisting of rows of a and b that have the same tag.
Duplicate values can be summed, or returned in pairs, or combined in any other
way specified by the join operation.

Example:
```
    auto df1 = DataFrame<int, float>{{1, 2, 3}, {10., 20., 30.}};
    auto df2 = DataFrame<int, float>{{1, 2, 3}, {-11., -22., -33.}};

    // g has tags 1,2,3 and values -1, -2, -3.
    auto g = Join::sum(df1, df2);

    // gp has tags 1,2,3 and values pair(10,-11), pair(20,-22), pair(30,-33).
    auto gp = Join::pair(df1, df2)
```

## Outer Join 

a: DataFrame<Tag, Ta>
b: DataFrame<Tag, Tb>
c = a[b] : DataFrame[Tag, Ta]


## Grouping

a: DataFrame<Tag, T>
b = group(a, reduction): DataFrame<Tag, Tacc>

Apply the reduction function to all entries of a that have the same tag. The
resulting dataframe has one row per unique tag in a, and the corresponding entry
is the result of the reduction.

```
    auto df = DataFrame<int, float>{{1, 2, 2, 3}, {10., 20., 100., 30.}};

    // g has tags 1,2,3 and values 10, 120, 30.
    auto g = Group::sum(df);
```