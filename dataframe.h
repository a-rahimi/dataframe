#include <algorithm>
#include <cmath>
#include <functional>
#include <memory>
#include <numeric>
#include <utility>
#include <vector>

// Ask clang-format to not sort the order of these. Their order is important
// because some of these depend on each other.
// clang-format off
#include "timer.h"
#include "expressions.h"
#include "formatting.h"
// clang-format on

struct RangeTag {};

struct NoTag {};

template <typename _T>
struct ConstantValue {};

/* A materialized dataframe.

Dataframes are light-weight to copy because they're copied by reference.
Dataframes contain at most two reference-counted pointers: one to a tags array
(unless the tags are RangeTags, in which the tags array is not explicitly
stored), and one to a values array (unless the value type is ConstantValue).
Copying a dataframe copies references to these arrays rather than their content.
Notably, modifying a copy of a dataframe modifies the tags and values of the
original.
*/
template <typename _Tag, typename _Value>
struct DataFrame : Operations<DataFrame<_Tag, _Value>> {
    using Tag = std::vector<_Tag>::value_type;
    using Value = std::vector<_Value>::value_type;

    std::shared_ptr<std::vector<_Tag>> tags;
    std::shared_ptr<std::vector<_Value>> values;

    DataFrame() : tags(new std::vector<_Tag>), values(new std::vector<_Value>) {}
    DataFrame(const std::vector<_Tag> &_tags, const std::vector<_Value> &_values)
        : tags(new auto(_tags)), values(new auto(_values)) {}

    size_t size() const { return tags->size(); }

    template <typename ValueOther>
    auto operator[](const DataFrame<Tag, ValueOther> &index) {
        return Expr_Intersection(to_expr(*this),
                                 to_expr(index),
                                 [](Tag, Value v, typename DataFrame<_Tag, ValueOther>::Value) { return v; });
    }

    auto operator[](size_t i) {
        struct TagValue {
            Tag t;
            Value &v;
        };
        return TagValue{(*tags)[i], (*values)[i]};
    }
    auto operator[](size_t i) const {
        struct TagValueConst {
            Tag t;
            const Value &v;
        };
        return TagValueConst{(*tags)[i], (*values)[i]};
    }
};

// A std::vector for sequential tags. Instead of storing the tag values,
// computes them as needed.
template <>
struct std::vector<RangeTag> {
    using value_type = size_t;

    size_t sz;

    vector(size_t _sz) : sz(_sz) {}
    vector() : sz(0) {}

    size_t size() const { return sz; }
    size_t operator[](size_t i) const { return i; }
};

// DataFrames of type ConstantValue use a special version of std::vector that takes up
// no space.
template <typename T>
struct std::vector<ConstantValue<T>> {
    using value_type = T;
    T v;
    const T &operator[](size_t i) const { return v; }
};