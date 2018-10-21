// Copyright (c) 2018 Dmitry Sokolov
//
// Distributed under the Boost Software License, Version 1.0. (See accompanying
// file LICENSE or copy at http://www.boost.org/LICENSE_1_0.txt)
//
#include "shared_string.hpp"

namespace utils {
namespace bits {

inline uint32_t reverse_bit_order(uint32_t x) {
    x = ((x >> 1) & 0x55555555) | ((x & 0x55555555) << 1);
    x = ((x >> 2) & 0x33333333) | ((x & 0x33333333) << 2);
    x = ((x >> 4) & 0x0F0F0F0F) | ((x & 0x0F0F0F0F) << 4);
    x = ((x >> 8) & 0x00FF00FF) | ((x & 0x00FF00FF) << 8);
    return (x >> 16) | (x << 16);
}

} // namespace bits

literal_dictionary::literal_dictionary() : mem_{pmr::get_default_resource()} {
}

// Dictionary singleton accessor.
literal_dictionary& literal_dictionary::global() {
    static literal_dictionary inst;
    return inst;
}

literal_dictionary::string literal_dictionary::add(string_view str) {
    return string(get_node(str));
}

// Add string to global dictionary.
const char* literal_dictionary::add_global_str(string_view str) {
    return global().get_node(str)->data();
}
literal_dictionary::string literal_dictionary::add_global(string_view str) {
    return string(global().get_node(str));
}

literal_dictionary::~literal_dictionary() {
    // Free hashtable memory.
    for(auto& segment : table_segments_) {
        if(segment.data == nullptr)
            break;
        auto alloc_size = (segment.table_size - segment.prev_table_size)
            * sizeof(literal_dictionary_node::ptr);
        mem_->deallocate(
            segment.data, alloc_size, std::alignment_of<node_ptr_t>::value);
    }
    // Free dictionary memory.
    auto* dict_page = allocated_pages_;
    while(dict_page) {
        auto* next = dict_page->next;
        mem_->deallocate(
            dict_page, allocate_chunk_size,
            std::alignment_of<node_ptr_t>::value);
        dict_page = next;
    }
}

// Dictionary node search/add method.
const literal_dictionary_node* literal_dictionary::get_node(string_view str) {
    if(str.empty())
        return &empty_node;
    auto hash = static_cast<uint32_t>(std::hash<string_view>()(str));
    const auto* segment = current_segment_.load();
    if(segment != nullptr) {
        // Fast lock-free search.
        auto table_size = segment->table_size;
        auto bucket_num = hash % table_size;
        while(bucket_num < segment->prev_table_size)
            --segment;
        auto segment_index = bucket_num - segment->prev_table_size;
        auto* node = segment->data[segment_index].load();
        while(node != nullptr) {
            if(node->hash == hash && node->str() == str)
                return node;
            if((node->hash % table_size) != bucket_num)
                break;
            node = node->next.load();
        }
    }
    // If node has not been found, add new one.
    return add_node(hash, str);
}

// Add new dictionary entry.
literal_dictionary_node* literal_dictionary::add_node(
    uint32_t hash, string_view str) {
    if(str.size() > max_string_size)
        throw std::runtime_error("dictionary dict_string to big");

    // Insertion should be mutually exclusive otherwise
    // duplicate allocations of the same dict_string may occur.
    std::lock_guard<std::mutex> lock(mtx_);
    if(current_version_ == 0 && table_segments_[0].table_size == 0)
        init_first_table_segment();
    // Increase table when load factor = 1.
    else if(
        table_segments_[current_version_].table_size == size_
        && current_version_ < table_segment_count - 1)
        init_next_table_segment();

    // Calculate bucket segment and segment position.
    auto* segment = &table_segments_[current_version_];
    auto table_size = segment->table_size;
    auto bucket_num = hash % table_size;
    while(bucket_num < segment->prev_table_size)
        --segment;
    auto segment_index = bucket_num - segment->prev_table_size;
    auto& bucket = segment->data[segment_index];
    // Find bucket insertion point (using reverse bit order).
    literal_dictionary_node* node = bucket.load();
    literal_dictionary_node* prev = nullptr;
    literal_dictionary_node* next = nullptr;
    if(node != nullptr) {
        auto shah = bits::reverse_bit_order(hash);
        do {
            // Check for bucket end.
            if((node->hash % table_size) != bucket_num)
                break;
            // Check equal, may be already inserted by concurrent thread.
            if(node->hash == hash && node->str() == str)
                return node;
            // Apply revers-bit ordering.
            if(next == nullptr) {
                auto node_shah = bits::reverse_bit_order(node->hash);
                if(shah < node_shah)
                    next = node;
                else
                    prev = node;
            }
            node = node->next.load();
        } while(node != nullptr);
    }
    // Just allocate new node and add to list.
    auto* new_node = allocate_node(hash, str);
    new_node->next = next;
    ++size_;
    if(prev != nullptr)
        prev->next.store(new_node);
    else
        bucket.store(new_node);
    return new_node;
}

// Allocate new dictionary node.
literal_dictionary_node* literal_dictionary::allocate_node(
    uint32_t hash, string_view str) {
    constexpr size_t node_align =
        std::alignment_of<literal_dictionary_node>::value;
    size_t node_size = sizeof(literal_dictionary_node) + str.size() + 1;
    if(current_page_ != nullptr)
        current_page_ =
            std::align(node_align, node_size, current_page_, remain_page_size_);
    // Allocate new page if no more space left.
    if(current_page_ == nullptr) {
        auto* page = static_cast<dict_page_t*>(mem_->allocate(
            allocate_chunk_size, std::alignment_of<dict_page_t>::value));
        total_allocated_size_ += allocate_chunk_size;
        page->next = allocated_pages_;
        allocated_pages_ = page;
        current_page_ = page + 1;
        remain_page_size_ = allocate_chunk_size - sizeof(dict_page_t);
        current_page_ =
            std::align(node_align, node_size, current_page_, remain_page_size_);
    }
    // Construct node and copy dict_string.
    auto* node = reinterpret_cast<literal_dictionary_node*>(current_page_);
    current_page_ = static_cast<char*>(current_page_) + node_size;
    remain_page_size_ -= node_size;
    new(node) literal_dictionary_node();
    node->hash = hash;
    node->size = static_cast<uint32_t>(str.size());
    char* data = reinterpret_cast<char*>(node + 1);
    std::char_traits<char>::copy(data, str.data(), str.size());
    data[str.size()] = '\0';
    return node;
}

// Allocate data for new table segment (uninitialized).
void literal_dictionary::allocate_table_segment(size_t segment_num) {
    auto& segment = table_segments_[segment_num];
    if(segment_num == 0)
        segment.table_size = table_initial_size;
    else {
        segment.prev_table_size = table_initial_size << (segment_num - 1);
        segment.table_size = segment.prev_table_size << 1;
    }
    auto alloc_size = (segment.table_size - segment.prev_table_size)
        * sizeof(literal_dictionary_node::ptr);
    segment.data = static_cast<node_ptr_t*>(
        mem_->allocate(alloc_size, std::alignment_of<node_ptr_t>::value));
    total_allocated_size_ += alloc_size;
}

// Allocate and fill first table segment.
void literal_dictionary::init_first_table_segment() {
    allocate_table_segment(0);
    std::uninitialized_fill_n(
        table_segments_[0].data, table_initial_size, nullptr);
    current_segment_.store(&table_segments_[0]);
}

// Allocate and fill new table segment.
void literal_dictionary::init_next_table_segment() {
    allocate_table_segment(current_version_ + 1);
    auto& new_segment = table_segments_[current_version_ + 1];
    auto old_table_size = table_segments_[current_version_].table_size;
    auto new_table_size = new_segment.table_size;
    // Iterate over all segments and split their buckets
    // saving right part in new segment.
    auto* segment = &table_segments_[0];
    for(size_t i = 0; i < old_table_size; ++i) {
        if(i >= segment->table_size)
            ++segment;
        auto segment_index = i - segment->prev_table_size;
        literal_dictionary_node* right_bucket =
            segment->data[segment_index].load();
        // Find list split position.
        while(right_bucket != nullptr
              && (right_bucket->hash % old_table_size) == i
              && (right_bucket->hash % new_table_size) == i)
            right_bucket = right_bucket->next.load();
        new(&new_segment.data[i]) node_ptr_t(right_bucket);
    }
    ++current_version_;
    current_segment_.store(&new_segment);
}

literal_dictionary::iterator& literal_dictionary::iterator::operator++() {
    if(dict_ == nullptr)
        return *this;
    if(node_ == nullptr) {
        bucket_position_ = 0;
        position_ = 0;
        segment_ = &dict_->table_segments_[0];
        last_segment_ = dict_->current_segment_.load();
        if(last_segment_ == nullptr)
            return *this;
    }
    else {
        node_ = node_->next.load();
        if(node_ != nullptr
           && node_->hash % segment_->table_size == position_) {
            ++bucket_position_;
            return *this;
        }
        node_ = nullptr;
        bucket_position_ = 0;
        ++position_;
    }
    while(node_ == nullptr) {
        if(position_ >= segment_->table_size) {
            if(segment_ == last_segment_)
                return *this;
            ++segment_;
        }
        node_ = segment_->data[position_ - segment_->prev_table_size].load();
        if(node_ != nullptr && node_->hash % segment_->table_size == position_)
            return *this;
        ++position_;
    }
    return *this;
}

} // namespace utils
