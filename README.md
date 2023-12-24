# Introduction

Dataframes are data structures that facilitate statistical operations on
datasets. They borrow ideas from data query languages like SQL, but instead of
providing a standalone language, operate inside a host programming language.
Since their introduction to the S programming language in 1990, dataframes have
been enhanced and cleaned up in many ways. For example, the R programming
language introduced [dplyr](http://dplyr.tidyverse.org), which expands the
semantics of data frames, [Pandas](https://pandas.pydata.org) implements
dataframes for Python, and [Polars](https://www.pola.rs) implements them in
Rust.

Traditionally, dataframes are two-dimensional tables, like tables in relational
database, or like numerical matrices.  But unlike a matrix, a dataframe may contain
values of different types, and unlike traditional relational databases, the
table can be stored in columnar format (where each column is represented as a
vector of homogenous type), or in row format, where each row is a heterogenous
record.  Typical operations on a dataframe produce new dataframes by selecting
subsets of rows or columns of these tables (similar to SQL's "SELECT"), grouping
rows or columns and computing summary statistics on these groups (similar to
SQL's "GROUP BY"), or joining multiple dataframes by combining their rows
whenever some of their entries match (similar to SQL's "JOIN").

This package implements a new, simplified dataframe in C++ in a way that takes
advantage of C++'s static typing, templating system, and data structure
facilities.  The C++ compiler can find bugs in your statistical statistical
reasoning through type checking, and can generate very fast code by fusing
operations at compile time.

# How this package differs from other dataframe packages

Dataframes in R and Python are useful because they implement in native code
operations that would otherwise be slow in interpreted code.  For example,
applying a user-supplied Python function to every row of a Pandas dataframe is
slow, so Pandas implements a huge variety of operations in native code to avoid
calling Python callbacks.  In C++, this is not an issue.  Applying a
user-supplied C++ lambda to every row of a dataframe is not any slower than
applying a built-in method. That means a C++ dataframe library can focus on
implementing high level operations on dataframes rather than implementing all
basic operation imaginable.

Dataframes in Python store data efficiently in a way that avoids boxing and
unboxing primitive types like floats and ints into Python objects.  But in C++,
primitive types aren't boxed, so arrays of C++ objects can be stored and access
without overhead.  Furthermore, C++ structs already provide rich ways to
manipulate structured data that obviates the need for many of the features of a
dataframe.  For example, C++ already has facilities to combine two different
types of struct into one new struct and handle the resulting conflicting field
names through multiple inheritance.  But in interpreted languages, structured
data have to be represented through dynamic data structures that are slower to
manipulate. This is contrary to the approach [a well-established C++ dataframe
package](https://github.com/hosseinmoein/DataFrame) takes, where dataframes are
entirely dynamic objects with similar semantics as Pandas.  

Here are the overarching desgin goals of this package:

* Do as much work at compile-time as possible. The dataframes are statically
typed so there is no overhead to represent schemas, or to interpret datatypes at
run-time. Operations on dataframes can be fused together at compile time, so
that chaining operations doesn't result in large temporary dataframes to be
dymamically allocated.

* Easy to port to new compute backends, and easy to optimize.  The algebra on
a traditional dataframe is surprisingly complicated.  For example [this
analysis](https://escholarship.org/uc/item/9x5608wr) derives a dataframe API
with 14 entry points. The API for R's dplyr dataframe package ostensibly has
only five entry points ("mutate", "select", "filter", "summarize", and
"arrange"), but each entry point provides a plethora of options that radically
modify their behavior. Some of these entry points even provide a mini-language
of their own.  Most of the operations in this package reduce to a call to one of
three basic operations: Expr_Union, Expr_Intersection, and Expr_Reduce.  When
porting the package to a new compute substrate, like a distributed system or a
GPU, its enough to port these three operations and leave the rest of the code
intact.  The fact that most operations reduce to Expr_Union, Expr_Intersection,
and Expr_Reduce also makes it easy to optimize query plans, because it's easier
to reason about compositions of of three operators than compositions of a large
number of basic operations. 

* Simple semantics. The dataframe semantics in this package are slightly more
complicated than those of Polars, which does not have a notion of an index, but
significantly simpler than those of dplyr or Pandas. This package introduces the
concept of "tags" as an alternative to an index. Tags are described below. I am
hoping they are easier to work with than indices. When I read pandas code, I
find it hard to remember how a dataframe is indexed at a particular point in the
code, so I'm often surprised by the arguments of `loc[]` or `groupby(level)`.
Hopefully the combination of tags and explicit dataframe types avoid these
problems.

This package borrows and modifies the notion of an index from SQL and Pandas.
These indices are called "tags" here, and they are involved in every operation
the package implements, including join, groupby, and indexing.  In this package,
a dataframe is a vector of values, and each of these values is tagged with
another value:

```
DataFrame = vector[pair[Tag, Value]]
```

The entries in values can be any datatype, including plain datatypes like ints
or floats, or user-defined data-types like structs or classes.  These tags can
also be of any type, as long as they support comparison with "<".  The entries
in the dataframe are sorted in ascending order of their tags.  These tags serve
several purposes, including that of an index in traditional dataframes. By
default the entries are tagged by consecutive integers, so that the tags are
just the row id of each value in the array (in this case the tags are implied
and aren't actually stored).  Joining two dataframes amounts to combining
entries of the dataframes that have matching tags. The groupby operation reduces
all entries of a dataframe that have the same tag value.  Selecting entries of a
dataframe happens by collecting all entries that have a given set of tag values.


# Dataframe Operations

## Indexing (intersection)

DataFrames can be sliced and indexed similarly to how matrices can be indexed in
numerical packages like [Eigen](https://eigen.tuxfamily.org/) or
[numpy](http://numpy.org):

```
a: DataFrame<Tag, ValueA>
b: DataFrame<Tag, ValueB>
c: DataFrame<Tag, ValueA>

c = a[b] 
```

In the above, `c` becomes a dataframe that consists of rows of `a` that have the
same tag as `b`.  Duplicated tags in `b` result in duplicated tags in `c`.
Duplicated tags in `a` result in undefined behavior (in reality, the first entry
with the matching tag is used).

This operation is implemented under the hood with Expr_Intersect.

Example:
```
// A dataframe with tags 1...4, and values 10...40 in steps of 10.
auto df = DataFrame<int, float>{.tags={1, 2, 3, 4}, .values={10., 20., 30., 40.}};

// A dataframe with tags 2, 3, and values that will be ignored when indexing.
auto i = DataFrame<int, NoValue>{{2, 3}};

// g has two entries, with tags 2 and 3 and values 20. and 30.
auto g = df[i];
```

## Inner Join (intersection)

```
a: DataFrame<Tag, ValueA>
b: DataFrame<Tag, ValueB>
b: DataFrame<Tag, ValueC>

c = a.collate(b, collate_op)
```

Collate_op has signature

```
ValueA, ValueB -> ValueC
```

This operation is implemented under the hood with Expr_Intersect.

Returns a dataframe consisting of rows of `a` and `b` that have the same tag.
Duplicate values can be summed, or returned in pairs, or combined in any other
way specified by the collate_op.

Example:
```
auto df1 = DataFrame<int, float>{.tags={1, 2, 3}, .values={10., 20., 30.}};
auto df2 = DataFrame<int, float>{.tags={1, 2, 3}, .values={-11., -22., -33.}};

// gp has tags 1,2,3 and values pair(10,-11), pair(20,-22), pair(30,-33).
auto gp = df1.collate(df2, [](float v1, float v2){ return std::pair(v1, v2); })
```

## Concatenating dataframes (outer join)

```
a: DataFrame<Tag, Value>
b: DataFrame<Tag, Value>
c = a.concatenate(b) : DataFrame<Tag, Value>
```

Implemented using Expr_Union.

Example:

```
auto df1 = DataFrame<int, float>({1, 2, 3}, {10., 20., 30.});
auto df2 = DataFrame<int, float>({2, 4}, {21., 40.});

// g has tags (1, 2, 2, 3, 4) and values (10, 20, 21, 30, 40).
auto g = df1.concatenate(df2);
```

## Grouping (reduction)

The grouping operation is similar to groupby in SQL and traditional dataframes, except that the group ids must be the tags of the dataframe:

```
a: DataFrame<Tag, ValueA>
g: DataFrame<Tag, ValueG>

g = a.reduce_sum()

or more generaly,

g = a.reduce(reduce_op)
```

The first operation applies the reduction `sum` to all entries of `a` that have
the same tag. The resulting dataframe has one row per unique tag in `a`, and the
corresponding entry is the result of the reduction. The second operation more
generally applies a reduction operation to the entries of `a` that have the same
tag.

```
auto df = DataFrame<int, float>{.tags={1, 2, 2, 3}, .values={10., 20., 100., 30.}};

// g has tags 1,2,3 and values 10, 120, 30.
auto g = df.reduce_sum();
```

# Example of row-wise dataframe storage

The vignette below illustrates row-wise storage. We'll define two dataframes, each of whose elements is a struct:

```
struct O1
{
    std::string favorite_color;
    int num_toes;
};


struct O2
{
    int num_teeth;
};
```

We'll then define a dataframe whose elements are of the first type, and another dataframe that contains elements of the second type:

```
auto df1 = DataFrame<std::string, O1>{
    {"ali", "john"},
    {O1{.favorite_color = "green", .num_toes = 6}, O1{.favorite_color = "blue", .num_toes = 10}}
};
auto df2 = DataFrame<std::string, O2>{
    {"ali", "john"},
    {O2{.num_teeth = 18}, O2{.num_teeth = 32}}
};
```

To join these into one dataframe, we'll define a new type through C++'s builtin inheritence mechanism:

```
struct O3 : O1, O2
{
    O3(const O1 &o1, const O2 &o2) : O1(o1), O2(o2) {}
};

// g is a dataframe whose elements on of type O3.
auto g = df1.collate(
    df2,
    [](const O1 &left, const O2 &right) { return O3(left, right); }
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
auto toes_per_tooth = df.num_toes.collate(
    df.num_teeth,
    [](int a, int b) { return float(a) / b; }
);
```

# Under the Hood

Almost all the operations in this package  are defined in terms of three basic
operations:


* Expr_Union combines the rows of two dataframes into one dataframe whose
rows in sorted in order of their tags. This operation is analogous to an outer
join in SQL.

* Expr_Intersect combines two dataframes into one dataframe by selecting only
rows of the two dataframes that have the same tag, and combining the values of
these rows with a user-supplied function. This operation is analogous to an
inner join in SQL. 

* Expr_Reduce combines all the entries of a dataframe that have the same tag
into one entry. This combination operation is a reduction with a user-supplied
binary operator. This operation resembles a group-by operation in SQL.

Under the hood, these operators produce one entry of their output dataframe, and
return. When you chain together, say an Expr_Union and an Expr_Reduce, you're
actually constructing an object that generates one entry of a new dataframe at a
time by sequentially invoking Expr_Union, which then invokves Expr_Reduce for
each row of the dataframe. This way, we avoid producing a temporary dataframe
for Expr_Reduce first, and feeding this temporary to Expr_Union, only to then
delete Expr_Reduce's output.

Unlike traditional dataframes, this package does not take a position on row-wise
vs columnar storage. Its dataframe are 1D vectors.  When the elements of these
vectors are records, the dataframes behave like row-wise dataframes. When
several vectors of scalars are combined in a C++ struct, they behave like a
columnar store. See the section below for details.
