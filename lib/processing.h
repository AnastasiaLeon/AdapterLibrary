#pragma once

#include <algorithm>
#include <cctype>
#include <filesystem>
#include <fstream>
#include <functional>
#include <optional>
#include <sstream>
#include <string>
#include <utility>
#include <vector>
#include <iterator>
#include <memory>
#include <map>
#include <unordered_set>
#include <expected>
#include <type_traits>

template <typename Key, typename Value>
struct KV {
    Key key;
    Value value;
};

template <typename Base, typename Joined>
struct JoinResult {
    Base base;
    std::optional<Joined> joined;
    
    bool operator==(const JoinResult& other) const {
        return base == other.base && joined == other.joined;
    }
};

template <typename Flow>
class DataFlow;

namespace detail {
    template <typename T>
    struct IsOptional : std::false_type {};
    template <typename T>
    struct IsOptional<std::optional<T>> : std::true_type {};
    template <typename T>
    constexpr bool IsOptionalV = IsOptional<T>::value;
    
    template <typename T>
    struct IsDataFlow : std::false_type {};
    template <typename T>
    struct IsDataFlow<DataFlow<T>> : std::true_type {};
    template <typename T>
    constexpr bool IsDataFlowV = IsDataFlow<T>::value;

    template <typename T>
    struct IsExpected : std::false_type {};
    template <typename T, typename E>
    struct IsExpected<std::expected<T, E>> : std::true_type {};
    template <typename T>
    constexpr bool IsExpectedV = IsExpected<T>::value;

    struct TransformTag {};
    struct NoTransformTag {};
    struct PassThroughTag {};
}

template <typename Container>
class DataFlowSource { // Base container adapter (simple wrapper over any container)
public:
    using iterator = typename Container::iterator;
    using const_iterator = typename Container::const_iterator;
    using value_type = typename Container::value_type;

    template <typename C>
    explicit DataFlowSource(C&& container) : container_(std::forward<C>(container)) {}

    iterator begin() { return container_.begin(); }
    iterator end() { return container_.end(); }
    const_iterator begin() const { return container_.begin(); }
    const_iterator end() const { return container_.end(); }

private:
    Container container_;
};

template <typename Flow>
class DataFlow { // Main data flow class
public:
    using iterator = typename Flow::iterator;
    using const_iterator = typename Flow::const_iterator;
    using value_type = typename Flow::value_type;

    explicit DataFlow(Flow flow) : flow_(std::move(flow)) {}

    iterator begin() { return flow_.begin(); }
    iterator end() { return flow_.end(); }
    const_iterator begin() const { return flow_.begin(); }
    const_iterator end() const { return flow_.end(); }

    std::vector<value_type> to_vector() const {
        return std::vector<value_type>(begin(), end());
    }

    bool operator==(const std::vector<value_type>& other) const {
        return to_vector() == other;
    }

private:
    Flow flow_;
};

template <typename Container>
auto AsDataFlow(Container&& container) { // Factory function
    using ContainerType = std::decay_t<Container>;
    if constexpr (std::is_same_v<typename ContainerType::value_type, std::stringstream>) { // Check if two types are identical
        std::vector<std::string> contents;
        for (auto& ss : container) {
            contents.push_back(ss.str());
        }
        return DataFlow<DataFlowSource<std::vector<std::string>>>(
            DataFlowSource<std::vector<std::string>>(std::move(contents))
        );
    } else {
        return DataFlow<DataFlowSource<ContainerType>>(
            DataFlowSource<ContainerType>(std::forward<Container>(container))
        );
    }
}

template <typename Prev>
class SplitFlow { // Main class for splitting
public:
    class Iterator;
    
    using iterator = Iterator;
    using value_type = std::string;

    SplitFlow(Prev prev, std::string delimiters)
        : prev_(std::move(prev)), delimiters_(std::move(delimiters)) {}

    Iterator begin() { return Iterator(prev_.begin(), prev_.end(), delimiters_); }
    Iterator end() { return Iterator(prev_.end(), prev_.end(), delimiters_); }

    class Iterator {
    public:
        using iterator_category = std::forward_iterator_tag;
        using value_type = std::string;
        using difference_type = std::ptrdiff_t;
        using pointer = value_type*;
        using reference = value_type&;

        Iterator(typename Prev::iterator current, typename Prev::iterator end, std::string delimiters)
            : current_(current), end_(end), delimiters_(std::move(delimiters)) {
            if (current_ != end_) {
                current_content_ = *current_;
                current_pos_ = 0;
                next_token();
            }
        }

        value_type operator*() const { return current_token_; }

        Iterator& operator++() {
            next_token();
            return *this;
        }

        bool operator!=(const Iterator& other) const {
            return !(current_ == other.current_ && current_pos_ == std::string::npos);
        }

    private:
        void next_token() {
            current_token_.clear();
            
            if (current_ == end_) {
                return;
            }

            if (current_pos_ == std::string::npos) {
                ++current_;
                if (current_ != end_) {
                    current_content_ = *current_;
                    current_pos_ = 0;
                } else {
                    return;
                }
            }

            size_t end_pos = current_content_.find_first_of(delimiters_, current_pos_);
            
            if (end_pos == std::string::npos) { // End of string
                current_token_ = current_content_.substr(current_pos_); // Substring
                current_pos_ = std::string::npos;
            } else {
                current_token_ = current_content_.substr(current_pos_, end_pos - current_pos_);
                current_pos_ = end_pos + 1;
            }
        }
        typename Prev::iterator current_;
        typename Prev::iterator end_;
        std::string delimiters_;
        std::string current_token_;
        std::string current_content_;
        size_t current_pos_ = 0;
    };

private:
    Prev prev_;
    std::string delimiters_;
};

class SplitAdapter { // Factory for splitting
public:
    explicit SplitAdapter(std::string delimiters) : delimiters_(std::move(delimiters)) {}

    template <typename Prev>
    auto operator()(Prev&& prev) const {
        return SplitFlow<Prev>(std::forward<Prev>(prev), delimiters_);
    }

private:
    std::string delimiters_;
};

inline auto Split(std::string delimiters) {
    return SplitAdapter(std::move(delimiters)); // Wrapper over constructor
}

template <typename Prev, typename Predicate>
class FilteredFlow { // Main class for filtering
public:
    class Iterator;
    
    using iterator = Iterator;
    using value_type = typename Prev::value_type;

    FilteredFlow(Prev prev, Predicate predicate)
        : prev_(std::move(prev)), predicate_(std::move(predicate)) {}

    Iterator begin() { return Iterator(prev_.begin(), prev_.end(), predicate_); }
    Iterator end() { return Iterator(prev_.end(), prev_.end(), predicate_); }

    class Iterator { // Iterator with filtering
    public:
        using iterator_category = std::forward_iterator_tag;
        using value_type = typename Prev::value_type;
        using difference_type = std::ptrdiff_t;
        using pointer = value_type*;
        using reference = value_type&;

        Iterator(typename Prev::iterator current, typename Prev::iterator end, Predicate predicate)
            : current_(current), end_(end), predicate_(std::move(predicate)) {
            skip_unmatched();
        }

        auto operator*() const { return *current_; }

        Iterator& operator++() {
            ++current_;
            skip_unmatched();
            return *this;
        }

        bool operator!=(const Iterator& other) const {
            return current_ != other.current_;
        }

    private:
        void skip_unmatched() {
            while (current_ != end_ && !predicate_(*current_)) {
                ++current_;
            }
        }

        typename Prev::iterator current_;
        typename Prev::iterator end_;
        Predicate predicate_;
    };

private:
    Prev prev_;
    Predicate predicate_;
};

template <typename Predicate>
class FilterAdapter { // Adapter for creating filtered flow
public:
    explicit FilterAdapter(Predicate predicate) : predicate_(std::move(predicate)) {}

    template <typename Prev>
    auto operator()(Prev&& prev) const {
        return FilteredFlow<Prev, Predicate>(std::forward<Prev>(prev), predicate_);
    }

private:
    Predicate predicate_;
};

template <typename Predicate> // Factory for filter adapter
auto Filter(Predicate predicate) {
    return FilterAdapter<Predicate>(std::move(predicate));
}

template <typename Prev, typename Func>
class TransformedFlow { // Applies function to each element without creating intermediate container
public:
    class Iterator;
    
    using iterator = Iterator;
    using value_type = decltype(std::declval<Func>()(std::declval<typename Prev::value_type>()));

    TransformedFlow(Prev prev, Func func)
        : prev_(std::move(prev)), func_(std::move(func)) {}

    Iterator begin() { return Iterator(prev_.begin(), func_); }
    Iterator end() { return Iterator(prev_.end(), func_); }

    class Iterator {
    public:
        using iterator_category = std::forward_iterator_tag;
        using value_type = typename TransformedFlow::value_type;
        using difference_type = std::ptrdiff_t;
        using pointer = value_type*;
        using reference = value_type&;

        Iterator(typename Prev::iterator it, Func func) : it_(it), func_(std::move(func)) {}

        auto operator*() const { return func_(*it_); }

        Iterator& operator++() {
            ++it_;
            return *this;
        }

        bool operator!=(const Iterator& other) const { return it_ != other.it_; }

    private:
        typename Prev::iterator it_;
        Func func_;
    };

private:
    Prev prev_;
    Func func_;
};

class WriteAdapter { // Writes data to output stream
    std::ostream& os_;
    char delimiter_;
public:
    WriteAdapter(std::ostream& os, char delimiter) 
        : os_(os), delimiter_(delimiter) {}
    
    template<typename Prev>
    auto operator()(Prev&& prev) const {
        bool first = true;
        bool hasElements = false;
        for (auto&& elem : prev) {
            if (!first) {
                os_ << delimiter_;
            }
            os_ << elem;
            first = false;
            hasElements = true;
        }
        if (hasElements) {
            os_ << delimiter_;
        }
        return std::forward<Prev>(prev);
    }
};

inline auto Write(std::ostream& os, char delimiter) {
    return WriteAdapter(os, delimiter);
}

template <typename Func>
class TransformAdapter { // Adapter for creating transformed flow
public:
    explicit TransformAdapter(Func func) : func_(std::move(func)) {}

    template <typename Prev>
    auto operator()(Prev&& prev) const {
        return TransformedFlow<Prev, Func>(std::forward<Prev>(prev), func_);
    }

private:
    Func func_;
};

template <typename Func>
auto Transform(Func func) { // Factory for transform adapter
    return TransformAdapter<Func>(std::move(func));
}

struct DropNulloptAdapter { // Filters stream, unpacking all values
    template <typename Prev>
    auto operator()(Prev&& prev) const {
        using ValueType = typename std::decay_t<Prev>::value_type;
        static_assert(detail::IsOptionalV<ValueType>, "DropNullopt can only be used with optional values");

        return std::forward<Prev>(prev) // Returns stream
            | Filter([](const auto& opt) { return opt.has_value(); })
            | Transform([](const auto& opt) { return opt.value(); });
    }
};

inline auto DropNullopt() {
    return DropNulloptAdapter();
}

struct AsVectorAdapter { // Converts stream to materialized vector
    template <typename Prev>
    auto operator()(Prev&& prev) const {
        using ValueType = typename std::decay_t<Prev>::value_type;
        std::vector<ValueType> result;
        
        if constexpr (requires { prev.size(); }) {
            result.reserve(prev.size()); // Reserves memory if size is available
        }

        for (auto&& elem : prev) {
            result.push_back(std::forward<decltype(elem)>(elem)); // Stores all elements in vector
        }
        
        return result;
    }
};

inline auto AsVector() {
    return AsVectorAdapter{};
}

template <typename RightFlow, typename LeftKeySelector, typename RightKeySelector>
class JoinAdapter { // For custom keys (searches for matches by key from right stream, returns vector of JoinResult)
public:
    JoinAdapter(RightFlow&& right, LeftKeySelector&& left_key, RightKeySelector&& right_key)
        : right_(std::forward<RightFlow>(right)), 
          left_key_(std::forward<LeftKeySelector>(left_key)),
          right_key_(std::forward<RightKeySelector>(right_key)) {}

    template <typename LeftFlow>
    auto operator()(LeftFlow&& left) const {
        using LeftValue = typename std::decay_t<LeftFlow>::value_type;
        using RightValue = typename std::decay_t<RightFlow>::value_type;
        using ResultType = JoinResult<LeftValue, RightValue>;

        std::unordered_multimap<
            decltype(right_key_(std::declval<RightValue>())),
            RightValue
        > right_map;

        for (const auto& right_value : right_) {
            right_map.emplace(right_key_(right_value), right_value);
        }

        std::vector<ResultType> result;
        for (auto&& left_value : left) {
            auto left_key = left_key_(left_value);
            auto range = right_map.equal_range(left_key);

            if (range.first == range.second) {
                result.push_back(ResultType{std::forward<LeftValue>(left_value), std::nullopt});
            } else {
                for (auto it = range.first; it != range.second; ++it) {
                    result.push_back(ResultType{
                        std::forward<LeftValue>(left_value), 
                        it->second
                    });
                }
            }
        }

        return AsDataFlow(std::move(result));
    }

private:
    RightFlow right_;
    LeftKeySelector left_key_; // Supports different key types (selector functions)
    RightKeySelector right_key_;
};

template <typename RightFlow>
class SimpleJoinAdapter { // For key-value pairs (data already packed in KV pairs)
public:
    SimpleJoinAdapter(RightFlow&& right) : right_(std::forward<RightFlow>(right)) {}

    template <typename LeftFlow>
    auto operator()(LeftFlow&& left) const {
        using LeftValue = typename std::decay_t<LeftFlow>::value_type;
        using RightValue = typename std::decay_t<RightFlow>::value_type;
        using ResultType = JoinResult<
            decltype(std::declval<LeftValue>().value),
            decltype(std::declval<RightValue>().value)
        >;

        static_assert(
            std::is_same_v<decltype(std::declval<LeftValue>().key), decltype(std::declval<RightValue>().key)>,
            "Key types must match"
        );

        std::unordered_multimap<
            decltype(std::declval<RightValue>().key),
            decltype(std::declval<RightValue>().value)
        > right_map;

        for (const auto& right_value : right_) {
            right_map.emplace(right_value.key, right_value.value);
        }

        std::vector<ResultType> result;
        for (auto&& left_value : left) {
            auto range = right_map.equal_range(left_value.key);

            if (range.first == range.second) {
                result.push_back(ResultType{left_value.value, std::nullopt});
            } else {
                for (auto it = range.first; it != range.second; ++it) {
                    result.push_back(ResultType{left_value.value, it->second});
                }
            }
        }

        return AsDataFlow(std::move(result));
    }

private:
    RightFlow right_;
};

template <typename RightFlow> // Factory for key-value join
auto Join(RightFlow&& right) {
    return SimpleJoinAdapter<RightFlow>(std::forward<RightFlow>(right));
}

template <typename RightFlow, typename LeftKeySelector, typename RightKeySelector>
auto Join(RightFlow&& right, LeftKeySelector&& left_key, RightKeySelector&& right_key) { // Factory for custom key join
    return JoinAdapter<RightFlow, LeftKeySelector, RightKeySelector>(
        std::forward<RightFlow>(right),
        std::forward<LeftKeySelector>(left_key),
        std::forward<RightKeySelector>(right_key)
    );
}

template <typename InitialValue, typename Aggregator, typename KeySelector>
class AggregateByKeyAdapter { // Groups elements by key, returns key-aggregated value pairs
public:
    AggregateByKeyAdapter(InitialValue initial_value, Aggregator aggregator, KeySelector key_selector)
        : initial_value_(std::move(initial_value)),
          aggregator_(std::move(aggregator)),
          key_selector_(std::move(key_selector)) {}

    template <typename Prev>
    auto operator()(Prev&& prev) const {
        using ValueType = typename std::decay_t<Prev>::value_type;
        using AccumulatedType = std::decay_t<InitialValue>;
        
        if constexpr (std::is_same_v<ValueType, std::string>) {
            std::unordered_map<std::string, AccumulatedType> aggregated_map;
            
            for (const auto& value : prev) {
                aggregated_map[value] = initial_value_;
            }
            
            for (const auto& value : prev) {
                aggregator_(aggregated_map[value], value);
            }
            
            std::vector<std::pair<std::string, AccumulatedType>> result;
            std::unordered_set<std::string> processed_keys;
            
            for (const auto& value : prev) {
                if (processed_keys.insert(value).second) {
                    result.emplace_back(value, aggregated_map[value]);
                }
            }
            
            return AsDataFlow(std::move(result));
        } else {
            using KeyType = decltype(key_selector_(std::declval<ValueType>()));
            using ResultType = std::pair<KeyType, AccumulatedType>;

            std::unordered_map<KeyType, AccumulatedType> aggregated_map;

            for (auto&& value : prev) {
                auto key = key_selector_(value);
                if (aggregated_map.find(key) == aggregated_map.end()) {
                    aggregated_map[key] = initial_value_;
                }
                aggregator_(aggregated_map[key], value);
            }

            std::vector<ResultType> result;
            std::unordered_set<KeyType> processed_keys;
            
            for (auto&& value : prev) {
                auto key = key_selector_(value);
                if (processed_keys.insert(key).second) {
                    result.emplace_back(key, std::move(aggregated_map[key]));
                }
            }

            return AsDataFlow(std::move(result));
        }
    }

private:
    InitialValue initial_value_;
    Aggregator aggregator_;
    KeySelector key_selector_;
};

template <typename InitialValue, typename Aggregator, typename KeySelector>
auto AggregateByKey(InitialValue initial_value, Aggregator aggregator, KeySelector key_selector) { // Factory for aggregation
    return AggregateByKeyAdapter<InitialValue, Aggregator, KeySelector>(
        std::move(initial_value),
        std::move(aggregator),
        std::move(key_selector)
    );
}

struct SplitExpectedBaseAdapter { // Splits data stream into two streams: values and errors
    template <typename Prev>
    auto operator()(Prev&& prev) const {
        using ValueType = typename std::decay_t<Prev>::value_type;
        static_assert(detail::IsExpectedV<ValueType>, 
            "SplitExpected() can only be used with expected values");
        
        using GoodType = typename ValueType::value_type;
        using BadType = typename ValueType::error_type;

        std::vector<GoodType> good_results; // All valid values
        std::vector<BadType> bad_results; // All errors

        for (auto&& value : prev) {
            if (value) {
                good_results.push_back(*std::move(value));
            } else {
                bad_results.push_back(std::move(value).error());
            }
        }

        return std::make_pair(
            AsDataFlow(std::move(bad_results)),
            AsDataFlow(std::move(good_results))
        );
    }
};

template <typename Func>
struct SplitExpectedTransformAdapter { // Splits existing expected or applies function returning expected and then splits
    Func func_;
    
    explicit SplitExpectedTransformAdapter(Func func) : func_(std::move(func)) {}

    template <typename Prev>
    auto operator()(Prev&& prev) const {
        using ValueType = typename std::decay_t<Prev>::value_type;
        
        if constexpr (detail::IsExpectedV<ValueType>) {
            return split_existing_expected(std::forward<Prev>(prev));
        } else {
            return apply_and_split(std::forward<Prev>(prev));
        }
    }

private:
    template <typename Prev>
    auto split_existing_expected(Prev&& prev) const { // Handles existing expected values
        using VT = typename std::decay_t<Prev>::value_type;
        std::vector<typename VT::error_type> errors;
        std::vector<typename VT::value_type> values;

        for (auto&& elem : prev) {
            if (elem.has_value()) {
                values.push_back(std::move(*elem));
            } else {
                errors.push_back(std::move(elem.error()));
            }
        }

        return std::make_pair(AsDataFlow(std::move(errors)), AsDataFlow(std::move(values)));
    }

    template <typename Prev>
    auto apply_and_split(Prev&& prev) const { // Applies function and splits resulting stream
        using ElemType = typename std::decay_t<Prev>::value_type;
        using ResultType = std::invoke_result_t<Func, ElemType>; // Type returned by func_
        static_assert(detail::IsExpectedV<ResultType>, "Function must return expected");

        std::vector<typename ResultType::error_type> errors;
        std::vector<typename ResultType::value_type> values;

        for (auto&& elem : prev) {
            auto res = func_(std::forward<decltype(elem)>(elem)); // func_ (passed in constructor) applied to each element, forward preserves value category
            if (res.has_value()) {
                values.push_back(std::move(*res));
            } else {
                errors.push_back(std::move(res.error()));
            }
        }

        return std::make_pair(AsDataFlow(std::move(errors)), AsDataFlow(std::move(values)));
    }
};

template <typename Func>
auto SplitExpected(Func&& func) { // Factory (adapter with custom transform function)
    return SplitExpectedTransformAdapter<std::decay_t<Func>>(std::forward<Func>(func));
}

inline auto SplitExpected() {  // Without transform function
    return SplitExpectedTransformAdapter([](auto&& x) -> decltype(auto) { 
        return std::forward<decltype(x)>(x); 
    });
}

template <typename Prev, typename Adapter>
auto operator|(Prev&& prev, Adapter&& adapter) {
    return std::forward<Adapter>(adapter)(std::forward<Prev>(prev));
}

class DirAdapter { // Iterable adapter for filesystem traversal
    std::string path_;
    bool recursive_;
public:
    using value_type = std::filesystem::path;

    DirAdapter(std::string path, bool recursive) 
        : path_(std::move(path)), recursive_(recursive) {}

    class iterator { // Iterator for file traversal
        struct IteratorVariant {
            std::filesystem::directory_iterator dir_it;
            std::filesystem::recursive_directory_iterator rec_it;
            bool is_recursive;
        } it_;
        
    public:
        using iterator_category = std::forward_iterator_tag;
        using value_type = std::filesystem::path;
        using difference_type = std::ptrdiff_t;
        using pointer = value_type*;
        using reference = value_type&;

        iterator() = default;

        explicit iterator(std::filesystem::directory_iterator it, bool recursive)
            : it_{it, {}, recursive} {}

        explicit iterator(std::filesystem::recursive_directory_iterator it, bool recursive)
            : it_{{}, it, recursive} {}

        value_type operator*() const { // Access current element
            return it_.is_recursive ? *it_.rec_it : *it_.dir_it;
        }

        iterator& operator++() { // Move to next element
            if (it_.is_recursive) {
                ++it_.rec_it;
            } else {
                ++it_.dir_it;
            }
            return *this;
        }

        bool operator!=(const iterator& other) const { // Compare iterators
            if (it_.is_recursive != other.it_.is_recursive) {
                return true;
            }
            return it_.is_recursive 
                ? (it_.rec_it != other.it_.rec_it)
                : (it_.dir_it != other.it_.dir_it);
        }
    };

    iterator begin() const { // Get starting iterator
        try {
            if (recursive_) {
                return iterator(std::filesystem::recursive_directory_iterator(path_), true);
            }
            return iterator(std::filesystem::directory_iterator(path_), false);
        } catch (...) {
            return end();
        }
    }

    iterator end() const { // Get ending iterator
        if (recursive_) {
            return iterator(std::filesystem::recursive_directory_iterator(), true);
        }
        return iterator(std::filesystem::directory_iterator(), false);
    }
};

inline auto Dir(std::string path, bool recursive = false) { // Factory for directory adapter
    return DirAdapter(std::move(path), recursive);
}

struct OpenFilesAdapter { // For opening files
    template<typename Prev>
    auto operator()(Prev&& prev) const {
        std::vector<std::ifstream> files;
        for (auto&& path : prev) {
            files.emplace_back(path);
        }
        return AsDataFlow(std::move(files));
    }
};

inline auto OpenFiles() { // Creates OpenFilesAdapter instance
    return OpenFilesAdapter();
}

class OutAdapter { // For outputting data
    std::ostream& os_;
public:
    OutAdapter(std::ostream& os) : os_(os) {} // Reference to output stream
    
    template<typename Prev>
    auto operator()(Prev&& prev) const {
        for (auto&& elem : prev) {
            os_ << elem << '\n';
        }
        return std::forward<Prev>(prev);
    }
};

inline auto Out(std::ostream& os) {
    return OutAdapter(os);
}