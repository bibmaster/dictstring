# Shared string dictionary.

String dictionary with lock-free read access. New items addition is guarded by mutex.
Internal dictionary storage is based on [split-ordered list hashtable](http://people.csail.mit.edu/shanir/publications/Split-Ordered_Lists.pdf).
```c++
utils::dict_string str1 = "foo";
utils::dict_string str2 = "bar";
// literal_dictionary::global() --> ["foo", "bar"]

// reuse strings without additional memory allocations:
utils::dict_string str2 = "foo";
utils::dict_string str3 = "bar";
// literal_dictionary::global() --> ["foo", "bar"]
```

Only primary dictionary filling takes cost, usage of already added strings is much less expensive.

For example, the time of sequential addition of 100,000 words from one source through 5 concurrent threads:
```
dictionary access time: 0.19141 seconds
dictionary access time: 0.0196399 seconds
dictionary access time: 0.0199463 seconds
```

