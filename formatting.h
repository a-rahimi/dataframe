#include <cerrno>
#include <cstdio>
#include <cstdlib>
#include <exception>
#include <fstream>
#include <iostream>
#include <memory>
#include <ranges>
#include <string>

// Forward declaration of a materialized dataframe.
template <typename Tag, typename Value>
struct DataFrame;

template <typename Tag, typename Value>
std::ostream& operator<<(std::ostream& s, const DataFrame<Tag, Value>& df) {
    for (size_t i = 0; i < df.size(); ++i)
        s << df[i].t << '\t' << df[i].v << std::endl;
    return s;
}

void from_string(int& v, const std::string_view& s) { v = std::atoi(s.begin()); }
void from_string(float& v, const std::string_view& s) { v = std::atof(s.begin()); }
void from_string(std::string& v, const std::string_view& s) { v = s; }

void parse_tab_separated_string(const std::string_view& s) { std::cout << "Extra stuff left: '" << s << "\'\n"; }

template <typename T, typename... Types>
void parse_tab_separated_string(const std::string_view& s, T& v, Types&... others) {
    auto i_end = s.find_first_of('\t');
    from_string(v, s.substr(0, i_end));

    if (i_end != std::string_view::npos)
        parse_tab_separated_string(s.substr(i_end + 1), others...);
}

std::string_view get_line(char* line, int max_line_length, FILE* f) {
    char* r = std::fgets(line, max_line_length, f);
    if (r) {
        // confirm the last character is a newline. otherwise the buffer was too
        // short to hold a line.
        std::string_view sv(line);
        if (sv.back() != '\n')
            throw std::runtime_error("Line exceeds buffer. Consider increasing the buffer size.");
        return sv;
    } else {
        return std::string_view("");
    }
}

template <std::ranges::range Container>
void read_tsv(Container& records, const std::string& tsv_filename, int header_lines = 1, int max_line_length = 5000) {
    auto tsv = std::unique_ptr<FILE, decltype(&std::fclose)>(std::fopen(tsv_filename.c_str(), "r"), &std::fclose);
    if (!tsv)
        throw std::system_error(errno, std::system_category(), tsv_filename);

    char line[max_line_length];

    // skip the header
    for (int i = 0; i < header_lines; ++i)
        get_line(line, sizeof(line), tsv.get());

    while (true) {
        auto line_string_view = get_line(line, sizeof(line), tsv.get());
        if (line_string_view.empty())
            break;

        typename Container::value_type item;
        from_tab_separated_string(item, line_string_view);
        records.push_back(item);
    }
}

template <typename T, typename... TSVArgs>
DataFrame<RangeTag, T> read_tsv(const std::string& tsv_filename, TSVArgs... args) {
    DataFrame<RangeTag, T> df;
    read_tsv(*df.values, tsv_filename, args...);
    df.tags->sz = df.values->size();
    return df;
}