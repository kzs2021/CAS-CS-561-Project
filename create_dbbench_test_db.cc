#include <cstdio>
#include <string>
#include <time.h>
#include <ctime>
#include <iostream>
#include <set>

#include "rocksdb/db.h"
#include "rocksdb/slice.h"
#include "rocksdb/options.h"

using ROCKSDB_NAMESPACE::DB;
using ROCKSDB_NAMESPACE::Options;
using ROCKSDB_NAMESPACE::PinnableSlice;
using ROCKSDB_NAMESPACE::ReadOptions;
using ROCKSDB_NAMESPACE::Status;
using ROCKSDB_NAMESPACE::WriteBatch;
using ROCKSDB_NAMESPACE::WriteOptions;
using ROCKSDB_NAMESPACE::Slice;

// define the path of the project
std::string kDBPath = "/tmp/rocks_db_std_test";


// generate a fixed-length string
// source: https://stackoverflow.com/questions/440133/how-do-i-create-a-random-alpha-numeric-string-in-c
std::string randomString(const int len) {
    static const char characters[] =
        "0123456789"
        "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
        "abcdefghijklmnopqrstuvwxyz";
    std::string result;
    result.reserve(len);
    for (int i = 0; i < len; i++) {
        result += characters[rand() % (sizeof(characters) - 1)];
    }
    return result;
}

// add zeros to the left of a string if its number of digits are not enough
std::string fixDigit(const int len, std::string str) {
    if (str.length() >= len) {return str.substr(0, len);}
    int numCharToAdd = len - str.length();
    for (int i = 0; i < numCharToAdd; i++) {
        str = "0" + str;
    }
    return str;
}

// To access members of a structure, use the dot operator
// To access members of a structure through a pointer, use the arrow operator
int main() {
    // initialize the database and the options
    DB* db;
    Options options;
    // initialize the timing variables
    clock_t startTime = clock();
    clock_t endTime = clock();
    // optimization
    options.IncreaseParallelism();
    options.OptimizeLevelStyleCompaction();
    options.create_if_missing = true;  // create the DB if it's not already present
    //options.error_if_exists = true;  // raise an error if the DB already exists
    // open DB and check the status
    Status statusDB = DB::Open(options, kDBPath, &db);
    assert(statusDB.ok());  // make sure to check error

    // TEST: insert a range of distinct keys
    std::string dataKey;
    std::string dataValue;
    int rangeSize = 1000000;  // the number of key-value pairs to generate
    int valueLen = 500;  // the length of the values
    int lenRangeSize = std::to_string(rangeSize).length();
    startTime = clock();  // start time of this operation
    for (int i = 0; i < rangeSize; i++) {
        // set up the key
        dataKey = fixDigit(lenRangeSize, std::to_string(i));
        // set up the value, which is a random string
        dataValue = randomString(valueLen);
        statusDB = db->Put(WriteOptions(), dataKey, dataValue);
    }
    endTime = clock();  // end time of this operation
    printf("Insertion time: %.2fs\n", (double)(endTime - startTime) / CLOCKS_PER_SEC);
    assert(statusDB.ok());  // make sure to check error
    return 0;
}
