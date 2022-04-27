#include <cstdio>
#include <string>
#include <time.h>
#include <ctime>
#include <iostream>

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
#if defined(OS_WIN)
std::string kDBPath = "C:\\Windows\\TEMP\\rocksdb_project";
#else
std::string kDBPath = "/tmp/rocksdb_project";
#endif

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
	options.error_if_exists = true;  // raise an error if the DB already exists
	// open DB and check the status
	Status statusDB = DB::Open(options, kDBPath, &db);
	assert(statusDB.ok());  // make sure to check error

	// TEST: insert a range of distinct keys
	std::string dataKey;
	std::string dataValue;
	int rangeSize = 10000;  // the number of key-value pairs to generate
	int valueLen = 50;  // the length of the values
	startTime = clock();  // start time of this operation
	for (int i = 0; i < rangeSize; i++) {
		// set up the key
		dataKey = fixDigit(std::to_string(rangeSize).length(), std::to_string(i));
		// set up the value, which is a random string
		dataValue = randomString(valueLen);
		statusDB = db->Put(WriteOptions(), dataKey, dataValue);
		assert(statusDB.ok());  // make sure to check error
	}
	endTime = clock();  // end time of this operation
	printf("Insertion time: %.2fs\n", (double)(endTime - startTime) / CLOCKS_PER_SEC);

	// TEST: delete some ranges
	int rangeDelSize = 100;  // number of elements in each range delete
	int numRangeDel = 10;  // number of range deletes
	startTime = clock();  // start time of this operation
	// ranges to be deleted: 
	for (int i = 0; i < numRangeDel; i++) {
		Slice startDelete = ""; // set start (inclusive) and end (exclusive) of the range
		Slice endDelete = "";
		// native range delete, creating a range tombstone
		statusDB = db->DeleteRange(WriteOptions(), db->DefaultColumnFamily(), startDelete, endDelete);
	}
	endTime = clock();  // end time of this operation
	printf("Range deletion time: %.2fs\n", (double)(endTime - startTime) / CLOCKS_PER_SEC);

	// TEST: point read, check both present & invalidated keys, do not check non-existing keys
	std::string valueRead;  // retrieve the value inserted, check what "1" corresponds to
	statusDB = db->Get(ReadOptions(), "1", &valueRead);
	assert(statusDB.ok());  // make sure to check error
	assert(valueRead == "10");  // check that we retrieved the value

	// TEST: range read
	rocksdb::Iterator* iter = db->NewIterator(rocksdb::ReadOptions());  // the iterator to traverse the data
	std::string rangeReadStart = "";  // the starting point of the range
	std::string rangeReadEnd = "";  // the ending point of the range
	int count = 0;
	startTime = clock();  // start time of this operation
	for (iter->Seek(rangeReadStart); iter->Valid() && iter->key().ToString() < rangeReadEnd; iter->Next()) {
		std::cout << "RANGE [" << rangeReadStart << ", " << rangeReadEnd << "] " << iter->key().ToString() << ": " 
			 	  << iter->value().ToString() << std::endl;
		count++;
	}
	endTime = clock();  // end time of this operation
	std::cout << "Range read count: " << count << std::endl;
	printf("Range read time: %.2fs\n", (double)(endTime - startTime) / CLOCKS_PER_SEC);
	assert(iter->status().ok()); // Check for any errors found during the scan
	delete iter;  // delete the iterator

	// delete the database and end the test
	delete db;
	return 0;
}

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
	if (str.length() >= len) {return str;}
	int numCharToAdd = len - str.length();
	for (int i = 0; i < numCharToAdd; i++) {
		str = "0" + str;
	}
	return str;
}
