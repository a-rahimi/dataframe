#include <iostream>

// Forward declaration of a materialized dataframe.
template <typename Tag, typename Value>
struct DataFrame;

struct NoValue;

template <typename Tag, typename Value>
std::ostream &operator<<(std::ostream &s, const DataFrame<Tag, Value> &df) {
    for (size_t i = 0; i < df.size(); ++i)
        s << df.tags[i] << '\t' << df.values[i] << std::endl;
    return s;
}

template <typename Tag>
std::ostream &operator<<(std::ostream &s, const DataFrame<Tag, NoValue> &df) {
    s << '[';
    for (size_t i = 0; i < df.size(); ++i)
        s << df.tags[i] << ", ";
    s << ']';
    return s;
}
