#pragma once
// Copyright (c) 2018 Dmitry Sokolov
//
// Distributed under the Boost Software License, Version 1.0. (See accompanying
// file LICENSE or copy at http://www.boost.org/LICENSE_1_0.txt)
//
// String dictionary with lock-free read access
//
// Strings are stored in split-ordered list hashtable.
// Split ordered lists:
//   http://people.csail.mit.edu/shanir/publications/Split-Ordered_Lists.pdf
//
// Hash table is stored as dynamically allocated segment array.
// Each segment while added doubles total table size.
// Therefore table segment size grows exponentialy: N, N, 2*N, 4*N, 8*N etc.

#include <array>
#include <atomic>
#include <mutex>
#include <string_view>
#ifdef _MSC_VER
#include <memory_resource>
#else
#include <experimental/memory_resource>
#endif

namespace utils {

#ifdef _MSC_VER
namespace pmr = std::pmr;
#else
namespace pmr = std::experimental::pmr;
#endif

using string_view = std::string_view;

// Dictionary intrusive linked node header.
// String (null-terminated) is placed after header.
struct literal_dictionary_node {
    using ptr = std::atomic<literal_dictionary_node*>;
    constexpr literal_dictionary_node() noexcept
        : next{nullptr}, hash{0}, size{0} {
    }
    ptr next;
    uint32_t hash;
    uint32_t size;
    const char* data() const {
        return reinterpret_cast<const char*>(this + 1);
    }
    string_view str() const {
        return {data(), size};
    }
};

// Empty node for default dict_string initialization.
struct empty_literal_dictionary_node : literal_dictionary_node {
    constexpr empty_literal_dictionary_node() noexcept : term{0} {
    }
    const char term;
};

// String literal dictionary.
class literal_dictionary {
    using node_t = literal_dictionary_node;
    using node_ptr_t = node_t::ptr;

    // Dictionary table segment.
    struct dictionary_segment {
        literal_dictionary_node::ptr* data = nullptr;
        size_t table_size = 0;
        size_t prev_table_size = 0;
    };

    // Allocated memory chunk header.
    // Used for collecting allocated dictionary pages.
    struct dict_page_t {
        dict_page_t* next;
    };

public:
    class string;
    class iterator;

    // Common empty node instance.
    constexpr static inline empty_literal_dictionary_node empty_node{};

    // Dictionary allocation chunk, 64K.
    static constexpr size_t allocate_chunk_size = 64 * 1024;

    // Initial dictionary hashtable size, 64K (8K nodes).
    static constexpr size_t table_initial_size =
        allocate_chunk_size / sizeof(node_ptr_t);

    // Maximum table segment index.
    // Maximum table size equals table_initial_size * 2 ^ table_segment_count.
    static constexpr size_t table_segment_count = 16;

    // Dictionary strings size limit (should fit in memory chunk).
    static constexpr size_t max_string_size = allocate_chunk_size
        - sizeof(dict_page_t) - sizeof(literal_dictionary_node);

    static constexpr const char* empty_str() {
        return &literal_dictionary::empty_node.term;
    }

    // Dictionary iteration.
    iterator begin() const;
    iterator end() const;

    // Dictionary node search/add method.
    string add(string_view str);

    // Global dictionary operations.
    static literal_dictionary& global();
    static const char* add_global_str(string_view str);
    static string add_global(string_view str);

private:
    literal_dictionary();
    ~literal_dictionary();

    // Dictionary node search/add method.
    const literal_dictionary_node* get_node(string_view str);

    // Add new dictionary entry.
    literal_dictionary_node* add_node(uint32_t hash, string_view str);

    // Allocate new dictionary node.
    literal_dictionary_node* allocate_node(uint32_t hash, string_view str);

    // Allocate data for new table segment (uninitialized).
    void allocate_table_segment(size_t segment_num);

    // Allocate and fill first table segment.
    void init_first_table_segment();

    // Allocate and fill new table segment.
    void init_next_table_segment();

private:
    std::atomic<dictionary_segment*> current_segment_{nullptr};
    // Dictionary size.
    size_t size_ = 0;
    // Current table version (max segment number).
    size_t current_version_{0};
    // Hashtable segment array.
    std::array<dictionary_segment, table_segment_count> table_segments_;
    // Mutex for adding new strings to dictionary.
    std::mutex mtx_;
    // Memory allocation stuff.
    pmr::memory_resource* mem_;
    dict_page_t* allocated_pages_ = nullptr;
    void* current_page_ = nullptr;
    size_t remain_page_size_ = 0;
    size_t total_allocated_size_ = 0;
};

// Dictionary string.
class literal_dictionary::string {
    const literal_dictionary_node& get_node() const {
        return *(reinterpret_cast<const literal_dictionary_node*>(str_) - 1);
    }
    friend class literal_dictionary;
    string(const literal_dictionary_node* node) noexcept : str_(node->data()) {
    }

public:
    using size_type = string_view::size_type;

    constexpr string() noexcept : str_{literal_dictionary::empty_str()} {
    }
    constexpr string(const string& rhs) noexcept : str_(rhs.str_) {
    }
    string(string_view rhs) : str_(literal_dictionary::add_global_str(rhs)) {
    }
    string(const char* rhs) : str_(literal_dictionary::add_global_str({rhs})) {
    }
    string& operator=(const string& rhs) noexcept {
        str_ = rhs.str_;
        return *this;
    }
    string& operator=(const string_view& rhs) {
        str_ = literal_dictionary::add_global_str(rhs);
        return *this;
    }
    string& operator=(const char* rhs) {
        str_ = literal_dictionary::add_global_str(rhs);
        return *this;
    }
    void clear() noexcept {
        str_ = literal_dictionary::empty_str();
    }
    size_t hash() const noexcept {
        return get_node().hash;
    }
    const char* data() const noexcept {
        return str_;
    }
    const char* c_str() const noexcept {
        return str_;
    }
    size_type size() const noexcept {
        return get_node().size;
    }
    bool empty() const noexcept {
        return *str_ == 0;
    }
    string_view ref() const noexcept {
        return {str_, size()};
    }
    operator string_view() const noexcept {
        return ref();
    }
    int compare(const string_view& rhs) const noexcept {
        return ref().compare(rhs);
    }
    int compare(const string& rhs) const noexcept {
        return ref().compare(rhs.ref());
    }

private:
    const char* str_;
};

using dict_string = literal_dictionary::string;

inline std::basic_ostream<char>& operator<<(
    std::basic_ostream<char>& os, const dict_string& str) {
    return os << str.ref();
}

inline bool operator<(const dict_string& lhs, const dict_string& rhs) noexcept {
    return lhs.ref() < rhs.ref();
}
inline bool operator<=(
    const dict_string& lhs, const dict_string& rhs) noexcept {
    return lhs.ref() <= rhs.ref();
}
inline bool operator>(const dict_string& lhs, const dict_string& rhs) noexcept {
    return lhs.ref() > rhs.ref();
}
inline bool operator>=(
    const dict_string& lhs, const dict_string& rhs) noexcept {
    return lhs.ref() >= rhs.ref();
}
inline bool operator==(
    const dict_string& lhs, const dict_string& rhs) noexcept {
    return lhs.ref() == rhs.ref();
}
inline bool operator!=(
    const dict_string& lhs, const dict_string& rhs) noexcept {
    return lhs.ref() != rhs.ref();
}

inline bool operator<(const dict_string& lhs, const string_view& rhs) noexcept {
    return lhs.ref() < rhs;
}
inline bool operator<=(
    const dict_string& lhs, const string_view& rhs) noexcept {
    return lhs.ref() <= rhs;
}
inline bool operator>(const dict_string& lhs, const string_view& rhs) noexcept {
    return lhs.ref() > rhs;
}
inline bool operator>=(
    const dict_string& lhs, const string_view& rhs) noexcept {
    return lhs.ref() >= rhs;
}
inline bool operator==(
    const dict_string& lhs, const string_view& rhs) noexcept {
    return lhs.ref() == rhs;
}
inline bool operator!=(
    const dict_string& lhs, const string_view& rhs) noexcept {
    return lhs.ref() != rhs;
}

inline bool operator==(const dict_string& lhs, const char* rhs) noexcept {
    return lhs.ref() == string_view(rhs);
}
inline bool operator!=(const dict_string& lhs, const char* rhs) noexcept {
    return lhs.ref() != string_view(rhs);
}

inline bool operator<(const string_view& lhs, const dict_string& rhs) noexcept {
    return lhs < rhs.ref();
}
inline bool operator<=(
    const string_view& lhs, const dict_string& rhs) noexcept {
    return lhs <= rhs.ref();
}
inline bool operator>(const string_view& lhs, const dict_string& rhs) noexcept {
    return lhs > rhs.ref();
}
inline bool operator>=(
    const string_view& lhs, const dict_string& rhs) noexcept {
    return lhs >= rhs.ref();
}
inline bool operator==(
    const string_view& lhs, const dict_string& rhs) noexcept {
    return lhs == rhs.ref();
}
inline bool operator!=(
    const string_view& lhs, const dict_string& rhs) noexcept {
    return lhs != rhs.ref();
}

// Dictionary iteration.
class literal_dictionary::iterator {
public:
    iterator(const literal_dictionary* dict = nullptr) : dict_(dict) {
    }
    iterator& operator++();
    string operator*() const {
        return string{node_};
    }
    bool operator==(const iterator& rhs) const {
        return node_ == rhs.node_;
    }
    bool operator!=(const iterator& rhs) const {
        return node_ != rhs.node_;
    }
    size_t position() const {
        return position_;
    }
    size_t bucket_position() const {
        return bucket_position_;
    }
    size_t hash() const {
        return node_->hash;
    }

private:
    const literal_dictionary* dict_;
    const node_t* node_ = nullptr;
    const dictionary_segment* segment_ = nullptr;
    const dictionary_segment* last_segment_ = nullptr;
    size_t position_ = 0;
    size_t bucket_position_ = 0;
};

inline literal_dictionary::iterator literal_dictionary::begin() const {
    auto it = iterator(this);
    ++it;
    return it;
}
inline literal_dictionary::iterator literal_dictionary::end() const {
    return iterator(this);
}

} // namespace utils

namespace std {
template<>
struct hash<utils::dict_string> {
    using key_type = utils::dict_string;
    size_t operator()(const key_type& v) const {
        return v.hash();
    }
};
} // namespace std
