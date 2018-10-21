#include <algorithm>
#include <array>
#include <iomanip>
#include <iostream>
#include <iterator>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

#include "shared_string.hpp"

std::string random_string(size_t length) {
    auto randchar = []() -> char {
        const char charset[] =
            "0123456789"
            "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
            "abcdefghijklmnopqrstuvwxyz";
        const size_t max_index = (sizeof(charset) - 1);
        return charset[rand() % max_index];
    };
    std::string str(length, 0);
    std::generate_n(str.begin(), length, randchar);
    return str;
}

#define EXPECT_STREQ(str1, str2)                                     \
    if(std::string_view(str1) != std::string_view(str2)) {           \
        std::cerr << __LINE__ << ": \"" << std::string_view(str1)    \
                  << "\" != \"" << std::string_view(str1) << "\"\n"; \
        return false;                                                \
    }
#define EXPECT_EQ(v1, v2)                                                  \
    if((v1) != (v2)) {                                                     \
        std::cerr << __LINE__ << ": " << (v1) << " != " << (v2) << "\"\n"; \
        return false;                                                      \
    }

using dictionary_source_t = std::vector<std::string>;

bool check_dictionary_refill(const dictionary_source_t& dict) {
    constexpr size_t thread_count = 5;
    using dict_string_list_t = std::vector<utils::dict_string>;
    std::array<dict_string_list_t, thread_count> results;
    for(auto& result : results)
        result.reserve(dict.size());
    // fill dictionary in parallel
    std::array<std::thread, thread_count> threads;
    auto t1 = std::chrono::high_resolution_clock::now();
    for(size_t i = 0; i < threads.size(); ++i) {
        auto& result = results[i];
        threads[i] = std::thread(
            [&dict, &result] { result.assign(dict.begin(), dict.end()); });
    }
    for(auto& t : threads)
        t.join();
    auto t2 = std::chrono::high_resolution_clock::now();
    auto time_spent =
        std::chrono::duration_cast<std::chrono::duration<double>>(t2 - t1)
            .count();
    std::cout << "dict fill time:" << time_spent << " seconds\n";
    // check all threads have same results
    for(size_t i = 0; i < dict.size(); ++i) {
        const auto& str = dict[i];
        const auto& dict_str = results[0][i];
        EXPECT_STREQ(str.data(), dict_str.data());
        EXPECT_EQ(str.size(), dict_str.size());
    }
    for(size_t i = 1; i < results.size(); ++i) {
        for(size_t j = 0; j < dict.size(); ++j) {
            const auto& str1 = results[0][j];
            const auto& str2 = results[i][j];
            EXPECT_EQ(str1.data(), str2.data());
            EXPECT_EQ(str1.size(), str2.size());
        }
    }
    return true;
}

// Print dictionary content.
void print_dictionary(const utils::literal_dictionary& dict) {
    size_t word_count = 0;
    for(auto i = dict.begin(); i != dict.end(); ++i) {
        ++word_count;
        std::cout << std::setw(6);
        if(i.bucket_position() == 0)
            std::cout << std::dec << i.position();
        else
            std::cout << " ";
        std::cout << ' ' << std::hex << std::setw(8) << i.hash();
        std::cout << ' ' << '"' << *i << '"' << '\n';
    }
    std::cout << "===============================\n";
    std::cout << " dictionary size: " << std::dec << word_count << '\n';
}

int main() {
    std::string text =
        "Lorem ipsum dolor sit amet consectetur adipiscing elit sed do "
        "eiusmod tempor incididunt ut labore et dolore magna aliqua Ut enim "
        "ad minim veniam quis nostrud exercitation ullamco laboris nisi ut "
        "aliquip ex ea commodo consequat Duis aute irure dolor in "
        "reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla "
        "pariatur Excepteur sint occaecat cupidatat non proident sunt in "
        "culpa qui officia deserunt mollit anim id est laborum";
    std::istringstream iss(text);
    std::vector<utils::dict_string> words(
        std::istream_iterator<std::string>{iss},
        std::istream_iterator<std::string>());
    print_dictionary(utils::literal_dictionary::global());

    constexpr size_t dict_size = 100000;
    constexpr size_t word_size = 30;
    // generate random dictionary
    dictionary_source_t dict;
    dict.resize(dict_size);
    std::generate_n(dict.begin(), dict_size, [word_size] {
        return random_string(1 + (rand() % word_size));
    });
    bool ok = check_dictionary_refill(dict) && check_dictionary_refill(dict)
        && check_dictionary_refill(dict);

    return ok ? 0 : -1;
}
