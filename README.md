# AdapterLibrary
Adapter library for simplified work with algorithms, containers and files

## Description

An adapter library that provides a streamlined approach to working with algorithms, containers, and files. Its generalized design for algorithms and iterators enables more elegant solutions to common tasks.

For example, counting word frequencies across all text files in a directory can be implemented concisely:

```cpp
Dir(argv[1], recursive) 
    | Filter([](std::filesystem::path& p){ return p.extension() == ".txt"; })
    | OpenFiles()
    | Split("\n ,.;")
    | Transform(
        [](std::string& token) { 
            std::transform(token.begin(), token.end(), [](char c){return std::tolower(c);});
            return token;
        })
    | AggregateByKey(
        0uz, 
        [](const std::string&, size_t& count) { ++count;},
        [](const std::string& token) { return token;}
      )
    | Transform([](const std::pair<std::string, size_t>& stat) { return std::format("{} - {}", stat.first, stat.second);})
    | Out(std::cout);
```

A key distinction of this approach compared to traditional methods is its support for [lazy evaluation](https://ru.wikipedia.org/wiki/%D0%9B%D0%B5%D0%BD%D0%B8%D0%B2%D1%8B%D0%B5_%D0%B2%D1%8B%D1%87%D0%B8%D1%81%D0%BB%D0%B5%D0%BD%D0%B8%D1%8F), where intermediate objects don't own the underlying data. This concept is similarly implemented in standard classes like [std::string_view](https://en.cppreference.com/w/cpp/string/basic_string_view) and [std::span](https://en.cppreference.com/w/cpp/container/span)

## Implemented Adapters


* Dir - Iterates through all files in a directory (including subdirectories recursively)
* OpenFiles - Opens a file stream for each path from the previous adapter
* Split - Splits input streams using specified delimiters
* Out - Outputs data to a stream
* AsDataFlow - Converts a container into a data flow for processing
* Transform - Modifies elements (similar to std::transform)
* Filter - Filters elements based on a predicate
* AsVector - Collects results into a vector
* Jvoin - Merges two data flows by key (LEFT JOIN semantics)
* KV - Key-value structure for join operations
* JoinResult - Contains merged data from both flows
* DropNullopt - Filters out std::nullopt from optional streams
* SplitExpected - Branches processing for valid/error cases in std::expected flows
* AggregateByKey - Aggregates values by key (eager evaluation)
    * Example:
        ```cpp
        aggregator := 
        [int value{}](char c) { 
            value++; 
            return value; 
        }
        [ a, b, c, d, a, a, b, d ] -> [ (a, 3), (b, 2), (c,1), (d,1) ]
        ```

All adapters except AggregateByKey and Join maintain constant memory usage.

## Tests

All adapter functionality is covered by tests using the Google Test framework.