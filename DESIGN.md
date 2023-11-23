# Abstract

Traditionally, the groupby operation takes as input two vectors: a data vector,
and vector that identifies the group for each entry in the first vector. It then
produces a mapping between groups and the result of a reduction operation on
each group. The inputs and outputs are of different types, which means a data
processing package that provides a groupby operator has to support vectors and
mapping objects as different types. We are looking for data types that are
closed under groupby. This interface would hopefully be easier for users to
reason about, be less surprising to novice users, and terser for advanced ones.

Pandas and data.frame already offer a tidy solution to this problem. The
dataframe is a 2D data structure each of whose columns may contain values of
different types.  These data structures can represent vectors (a dataframe with
a single column), matrices, or mapping types (a dataframe where the first column
is a key and the second is the value). 


# Data types

A dataframe is tuple of two vectors: and tag vector, and the values vector:

Vector<T> = [v[0], ...], where v[i: int]: T.
DataFrame<Tag,T> = (tags: Vector<Tag>, values: Vector<T>)

We'll rely on some special subtypes of Vector to speed up certain operations. For example,

Range = Vector<int>, which contains arithmetic sequences of integers. We'll use
these as the default tag vector.

# Semantics

## General merge

a: DataFrame<Tag, Ta>
g: DataFrame<Tag, Tg>
c = merge(a , b, accumulate): DataFrame[Tag, Top]

The function `accumulate`` has signature

accumulate: (acc: Tacc, taga: Union[Tag, NoTag], tagb: Union[Tag, NoTag], va: Ta,  vb: Tb) -> Tag, Top

It is called on every tag `t` in union(a,b), so that 

   (taga=t or taga:NoTag) and (tagb=t or tagb:NoTag).

`acc` is the result of the reduction so far.

If the tag returned by `accumulate` has type NoTag, the result is not saved in c.

## Indexing 

a: DataFrame<Tag, Ta>
b: DataFrame<Tag, Tb>
c = a[b] : DataFrame[Tag, Ta]
  = merge(a, b, accumulate=index)

where the function 

index(Tacc acc, Tag a, Tag b, Ta a, Ta b) ->  Tag, Ta

returns (a and b, a), meaning that if either a or b are NoTag, it returns NoTag,
otherwise it returns the tag. The value returned is always from a.

## Inner Join 

a: DataFrame<Tag, Ta>
b: DataFrame<Tag, Tb>
c = a[b] : DataFrame[Tag, Ta]
  = merge(a, b, accumulate=innerjoin)

where the function 

innerjoin(Tacc acc, Tag a, Tag b, Ta a, Ta b) ->  Tag, Ta

returns (a and b, (a,b)), meaning that if either a or b are NoTag, it returns
NoTag, otherwise it returns the tag. The value returned is the pair formed from
the corresponding elements of a and b.

## Outer Join 

a: DataFrame<Tag, Ta>
b: DataFrame<Tag, Tb>
c = a[b] : DataFrame[Tag, Ta]
  = merge(a, b, accumulate=outerjoin)

where the function 

outerjoin(Tacc acc, Tag a, Tag b, Ta a, Ta b) ->  Tag, Ta

returns (a or b, (a, b)).

## Grouping

a: DataFrame<Tag, T>
b = group(a, reduction, initial: Tacc): DataFrame<Tag, Tacc>

where the function

reduction(acc: Tacc, v: T) -> Tacc