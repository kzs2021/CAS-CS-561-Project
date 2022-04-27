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
#if defined(OS_WIN)
std::string kDBPath = "C:\\Windows\\TEMP\\rocksdb_project";
#else
std::string kDBPath = "/tmp/rocksdb_project";
#endif

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
	int rangeSize = 10000;  // the number of key-value pairs to generate
	int valueLen = 50;  // the length of the values
	int lenRangeSize = std::to_string(rangeSize).length();
	startTime = clock();  // start time of this operation
	for (int i = 0; i < rangeSize; i++) {
		// set up the key
		dataKey = fixDigit(lenRangeSize, std::to_string(i));
		// set up the value, which is a random string
		dataValue = randomString(valueLen);
		statusDB = db->Put(WriteOptions(), dataKey, dataValue);
		assert(statusDB.ok());  // make sure to check error
	}
	endTime = clock();  // end time of this operation
	printf("Insertion time: %.2fs\n", (double)(endTime - startTime) / CLOCKS_PER_SEC);

	// TEST: point read, check both present & invalidated keys, do not check non-existing keys
	std::string keyRead;  // the key which the point query is interested in
	std::set<std::string> keyReadSetBefore;  // ensure that we do not repeatedly visit a key
	std::string valueRead;  // retrieve the value inserted, check what "1" corresponds to
	int numPointQueries = 1000;
	int countPointBefore = 0;
	// TEST: point read before deletion
	startTime = clock();  // start time of this operation
	// perform some random point queries
	for (int i = 0; i < numPointQueries; i++) {
		keyRead = fixDigit(lenRangeSize, std::to_string(rand() % rangeSize));
		while (keyReadSetBefore.count(keyRead) != 0) {
			keyRead = fixDigit(lenRangeSize, std::to_string(rand() % rangeSize));
		}
		statusDB = db->Get(ReadOptions(), keyRead, &valueRead);
		if (!statusDB.IsNotFound()) {countPointBefore++;}
		keyReadSetBefore.insert(keyRead);
	}
	endTime = clock();  // end time of this operation
	std::cout << "Point read before deletes count: " << countPointBefore << std::endl;
	printf("Point queries time before deletes: %.2fs\n", (double)(endTime - startTime) / CLOCKS_PER_SEC);
	assert(statusDB.ok());  // make sure to check error

	// TEST: range read 
	std::string rangeReadStart = "02500";  // the starting point of the range
	std::string rangeReadEnd = "07499";  // the ending point of the range
	// TEST: range read before deletion
	rocksdb::Iterator* iterBefore = db->NewIterator(rocksdb::ReadOptions());  // the iterator to traverse the data
	int countRangeReadBefore = 0;
	startTime = clock();  // start time of this operation
	for (iterBefore->Seek(rangeReadStart); iterBefore->Valid() && iterBefore->key().ToString() < rangeReadEnd; iterBefore->Next()) {
		std::cout << "RANGE [" << rangeReadStart << ", " << rangeReadEnd << "] " << iterBefore->key().ToString() << ": " 
			 	  << iterBefore->value().ToString() << std::endl;
		countRangeReadBefore++;
	}
	endTime = clock();  // end time of this operation
	std::cout << "Range read before deletes count: " << countRangeReadBefore << std::endl;
	printf("Range read before deletes time: %.2fs\n", (double)(endTime - startTime) / CLOCKS_PER_SEC);
	assert(iterBefore->status().ok()); // Check for any errors found during the scan
	delete iterBefore;  // delete the iterator

	// TEST: delete some ranges
	Slice startDelete;
	Slice endDelete;
	int rangeDelSize = 100;  // number of elements in each range delete
	int numRangeDel = 10;  // number of range deletes
	int startTemp = 100;
	startTime = clock();  // start time of this operation
	// ranges to be deleted: 
	for (int i = 0; i < numRangeDel; i++) {
		startDelete = fixDigit(lenRangeSize, std::to_string(startTemp)); // set start (inclusive) and end (exclusive) of the range
		endDelete = fixDigit(lenRangeSize, std::to_string(startTemp + rangeDelSize));
		// native range delete, creating a range tombstone
		statusDB = db->DeleteRange(WriteOptions(), db->DefaultColumnFamily(), startDelete, endDelete);
		startTemp += 1000;
	}
	endTime = clock();  // end time of this operation
	printf("Range deletion time: %.2fs\n", (double)(endTime - startTime) / CLOCKS_PER_SEC);

	// TEST: point query after deletion
	std::set<std::string> keyReadSetAfter;  // ensure that we do not repeatedly visit a key
	int countPointAfter = 0;
	startTime = clock();  // start time of this operation
	// perform some random point queries
	for (int i = 0; i < numPointQueries; i++) {
		keyRead = fixDigit(lenRangeSize, std::to_string(rand() % rangeSize));
		while (keyReadSetAfter.count(keyRead) != 0) {
			keyRead = fixDigit(lenRangeSize, std::to_string(rand() % rangeSize));
		}
		statusDB = db->Get(ReadOptions(), keyRead, &valueRead);
		if (!statusDB.IsNotFound()) {countPointAfter++;}
		keyReadSetAfter.insert(keyRead);
	}
	endTime = clock();  // end time of this operation
	std::cout << "Point read after deletes count: " << countPointAfter << std::endl;
	printf("Point queries time after deletes: %.2fs\n", (double)(endTime - startTime) / CLOCKS_PER_SEC);
	assert(statusDB.ok());  // make sure to check error

	// TEST: range read after deletes
	rocksdb::Iterator* iterAfter = db->NewIterator(rocksdb::ReadOptions());  // the iterator to traverse the data
	int countRangeReadAfter = 0;
	startTime = clock();  // start time of this operation
	for (iterAfter->Seek(rangeReadStart); iterAfter->Valid() && iterAfter->key().ToString() < rangeReadEnd; iterAfter->Next()) {
		std::cout << "RANGE [" << rangeReadStart << ", " << rangeReadEnd << "] " << iterAfter->key().ToString() << ": " 
			 	  << iterAfter->value().ToString() << std::endl;
		countRangeReadAfter++;
	}
	endTime = clock();  // end time of this operation
	std::cout << "Range read after deletes count: " << countRangeReadAfter << std::endl;
	printf("Range read after deletes time: %.2fs\n", (double)(endTime - startTime) / CLOCKS_PER_SEC);
	assert(iterAfter->status().ok()); // Check for any errors found during the scan
	delete iterAfter;  // delete the iterator

	// delete the database and end the test
	delete db;
	return 0;
}
