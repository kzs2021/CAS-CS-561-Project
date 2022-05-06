#include <cstdio>
#include <string>
#include <time.h>
#include <ctime>
#include <iostream>
#include <set>
#include <array>

#include "rocksdb/db.h"
#include "rocksdb/slice.h"
#include "rocksdb/options.h"
#include "rocksdb/iostats_context.h"
#include "rocksdb/perf_context.h"

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
	// open DB and check the status
	printf("Opening the DB...\n");
	Status statusDB = DB::Open(options, kDBPath, &db);
	assert(statusDB.ok());  // make sure to check error
	printf("DB opened.\n");
	// initialize the performance & I/O stats contexts
	rocksdb::SetPerfLevel(rocksdb::PerfLevel::kEnableTimeAndCPUTimeExceptForMutex);
	rocksdb::PerfContext perfContext = *(rocksdb::get_perf_context());
	rocksdb::IOStatsContext ioContext = *(rocksdb::get_iostats_context());
	perfContext.Reset();
	ioContext.Reset();
	// initialize approximate size info
	std::array<rocksdb::Range, 1> ranges;
  	std::array<uint64_t, 1> sizes;
  	rocksdb::SizeApproximationOptions SAoptions;
  	SAoptions.include_memtables = true;
  	SAoptions.files_size_error_margin = 0.1;

	// TEST: insert a range of distinct keys
	double insertTotalTime = 0.0;  // the total runtime of insertion
	std::string dataKey;
	std::string dataValue;
	int rangeSize = 10000;  // the number of key-value pairs to generate
	int numPointQueries = rangeSize/10;  // number of point queries to perform
	int valueLen = 1012;  // the length of the values
	//int keyLen = std::to_string(rangeSize).length();  // the length of each key
	int keyLen = 12;  // the length of each key
	// initialize the containers for the size info
	ranges[0].start = fixDigit(keyLen, std::to_string(0));
	ranges[0].limit = fixDigit(keyLen, std::to_string(rangeSize - 1));
	// generate the workload
	printf("Insertion started.\n");
	for (int i = 0; i < rangeSize; i++) {
		// set up the key
		dataKey = fixDigit(keyLen, std::to_string(i));
		// set up the value, which is a random string
		dataValue = randomString(valueLen);
		startTime = clock();  // start time of this single operation
		statusDB = db->Put(WriteOptions(), dataKey, dataValue);
		endTime = clock();  // end time of this single operation
		insertTotalTime += (double)(endTime - startTime) / CLOCKS_PER_SEC;
		assert(statusDB.ok());  // make sure to check error
	}
	printf("Insertion time: %.6fs\n", insertTotalTime);
	std::cout << rangeSize << " key-value pairs inserted." << std::endl;

	perfContext = *(rocksdb::get_perf_context());
	ioContext = *(rocksdb::get_iostats_context());
	rocksdb::SetPerfLevel(rocksdb::PerfLevel::kDisable);
	std::cout << "=======================================" << std::endl;
	std::cout << perfContext.ToString() << std::endl;
	std::cout << "=======================================" << std::endl;
	std::cout << ioContext.ToString() << std::endl;

  	statusDB = db->GetApproximateSizes(SAoptions, db->DefaultColumnFamily(), ranges.data(), 1, sizes.data());
	assert(statusDB.ok());  // make sure to check error
	std::cout << "Size after insertion: " << sizes[0] << " bytes" << std::endl;

	// perform some warm-up point queries here
	std::string keyReadTemp;
	std::string valueReadTemp;
	printf("Warn-up queries before deletes started.\n");
	for (int i = 0; i < numPointQueries; i++) {
		keyReadTemp = fixDigit(keyLen, std::to_string(rand() % rangeSize));
		statusDB = db->Get(ReadOptions(), keyReadTemp, &valueReadTemp);
		assert(statusDB.ok());  // make sure to check error
	}
	printf("Warn-up queries before deletes done.\n");

	// TEST: point read, check both present & invalidated keys, do not check non-existing keys
	std::string keyRead;  // the key which the point query is interested in
	std::set<std::string> keyReadSetBefore;  // ensure that we do not repeatedly visit a key
	std::string valueRead;  // retrieve the value inserted
	int countPointBefore = 0;  // count the number of valid entries retrieved
	// TEST: point read before deletion
	double pointReadTotalTime = 0.0;  // total time of the point queries
	// perform some random point queries
	printf("Point read before deletes started.\n");
	for (int i = 0; i < numPointQueries; i++) {
		keyRead = fixDigit(keyLen, std::to_string(rand() % rangeSize));
		// ensure that we do not re-read an entry
		while (keyReadSetBefore.count(keyRead) != 0) {
			keyRead = fixDigit(keyLen, std::to_string(rand() % rangeSize));
		}
		startTime = clock();  // start time of this single operation
		statusDB = db->Get(ReadOptions(), keyRead, &valueRead);
		endTime = clock();  // end time of this single operation
		assert(statusDB.ok());  // make sure to check error
		pointReadTotalTime += (double)(endTime - startTime) / CLOCKS_PER_SEC;
		if (!statusDB.IsNotFound()) {countPointBefore++;}
		keyReadSetBefore.insert(keyRead);
	}
	std::cout << "Point read before deletes count: " << countPointBefore << std::endl;
	printf("Point queries runtime before deletes: %.6fs\n", pointReadTotalTime);
	printf("Read throughput before deletes: %.6fentries/s\n", countPointBefore/pointReadTotalTime);
	
	perfContext = *(rocksdb::get_perf_context());
	ioContext = *(rocksdb::get_iostats_context());
	rocksdb::SetPerfLevel(rocksdb::PerfLevel::kDisable);
	std::cout << "=======================================" << std::endl;
	std::cout << perfContext.ToString() << std::endl;
	std::cout << "=======================================" << std::endl;
	std::cout << ioContext.ToString() << std::endl;

	statusDB = db->GetApproximateSizes(SAoptions, db->DefaultColumnFamily(), ranges.data(), 1, sizes.data());
	assert(statusDB.ok());  // make sure to check error
	std::cout << "Size after 1st read: " << sizes[0] << " bytes" << std::endl;

	// delete many small ranges
	Slice startDelete;
	Slice endDelete;
	int rangeDelSize = rangeSize/100;  // number of elements in each range delete
	int numRangeDel = 10;  // number of range deletes
	int startTemp = rangeSize/100;  // initial starting point of the deletes
	double rangeDelTotalTime = 0.0;  // total time of the deletes
	// ranges to be deleted: 
	for (int i = 0; i < numRangeDel; i++) {
		// set start (inclusive) and end (exclusive) of the range
		startDelete = fixDigit(keyLen, std::to_string(startTemp));
		endDelete = fixDigit(keyLen, std::to_string(startTemp + rangeDelSize + 1));
		// native range delete, creating a range tombstone
		startTime = clock();  // start time of this operation
		statusDB = db->DeleteRange(WriteOptions(), db->DefaultColumnFamily(), startDelete, endDelete);
		endTime = clock();  // end time of this operation
		assert(statusDB.ok());  // make sure to check error
		rangeDelTotalTime += (double)(endTime - startTime) / CLOCKS_PER_SEC;
		std::cout << "RANGE DELETED [" << startDelete.ToString() << ", " << endDelete.ToString() << "] " << std::endl;
		startTemp += rangeSize/10;
	}
	printf("Small range deletion time: %.6fs\n", rangeDelTotalTime);
	// get the size info
	statusDB = db->GetApproximateSizes(SAoptions, db->DefaultColumnFamily(), ranges.data(), 1, sizes.data());
	assert(statusDB.ok());  // make sure to check error
	std::cout << "Size after deletes: " << sizes[0] << " bytes" << std::endl;
	
	// perform some warm-up point queries here
	printf("Warn-up queries after deletes started.\n");
	for (int i = 0; i < numPointQueries; i++) {
		keyReadTemp = fixDigit(keyLen, std::to_string(rand() % rangeSize));
		statusDB = db->Get(ReadOptions(), keyReadTemp, &valueReadTemp);
		if (!statusDB.IsNotFound()) {assert(statusDB.ok());}  // make sure to check error
	}
	printf("Warn-up queries after deletes done.\n");

	// TEST: point query after deletion
	std::set<std::string> keyReadSetAfter;  // ensure that we do not repeatedly visit a key
	int countPointValidAfter = 0;  // count the number of valid entries retrieved
	int countPointInvalidAfter = 0;  // count the number of invalid entries retrieved
	double pointReadTotalTimeAfter = 0.0;  // total time of the point queries
	double validPointReadTotalTime = 0.0;
	double invalidPointReadTotalTime = 0.0;
	printf("Point read after deletes started.\n");
	// perform some random point queries
	for (int i = 0; i < numPointQueries; i++) {
		keyRead = fixDigit(keyLen, std::to_string(rand() % rangeSize));
		while (keyReadSetAfter.count(keyRead) != 0) {
			keyRead = fixDigit(keyLen, std::to_string(rand() % rangeSize));
		}
		startTime = clock();  // start time of this operation
		statusDB = db->Get(ReadOptions(), keyRead, &valueRead);
		endTime = clock();  // end time of this operation
		pointReadTotalTimeAfter += (double)(endTime - startTime) / CLOCKS_PER_SEC;
		if (!statusDB.IsNotFound()) {
			assert(statusDB.ok());  // make sure to check error, ignore the case where the key is not found
			validPointReadTotalTime += (double)(endTime - startTime) / CLOCKS_PER_SEC;
			countPointValidAfter++;
		}
		else {
			invalidPointReadTotalTime += (double)(endTime - startTime) / CLOCKS_PER_SEC;
			countPointInvalidAfter++;
		}
		keyReadSetAfter.insert(keyRead);
	}
	std::cout << "Point read (valid) after deletes count: " << countPointValidAfter << std::endl;
	std::cout << "Point read (invalid) after deletes count: " << countPointInvalidAfter << std::endl;
	printf("Point queries time after deletes: %.6fs\n", pointReadTotalTimeAfter);
	printf("Time spent for reading valid entries: %.6fs\n", validPointReadTotalTime);
	printf("Time spent for reading invalid entries: %.6fs\n", invalidPointReadTotalTime);
	printf("Average read throughput after deletes: %.6f entries/s\n", numPointQueries/pointReadTotalTimeAfter);
	printf("Read throughput (valid) after deletes: %.6f entries/s\n", countPointValidAfter/validPointReadTotalTime);
	printf("Read throughput (invalid) after deletes: %.6f entries/s\n", countPointInvalidAfter/invalidPointReadTotalTime);
	
	perfContext = *(rocksdb::get_perf_context());
	ioContext = *(rocksdb::get_iostats_context());
	rocksdb::SetPerfLevel(rocksdb::PerfLevel::kDisable);
	std::cout << "=======================================" << std::endl;
	std::cout << perfContext.ToString() << std::endl;
	std::cout << "=======================================" << std::endl;
	std::cout << ioContext.ToString() << std::endl;

	statusDB = db->GetApproximateSizes(SAoptions, db->DefaultColumnFamily(), ranges.data(), 1, sizes.data());
	assert(statusDB.ok());  // make sure to check error
	std::cout << "Size after 2nd read: " << sizes[0] << " bytes" << std::endl;
	
	// delete the database and end the test
	delete db;
	return 0;
}
