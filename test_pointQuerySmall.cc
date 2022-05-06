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
// reference: https://stackoverflow.com/questions/440133/how-do-i-create-a-random-alpha-numeric-string-in-c
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

// do some warm-up Queries
void warmUp(Status statusDB, DB* db, int rangeSize, int keyLen, int warmUpNum, std::string info) {
	std::string keyReadTemp;
	std::string valueReadTemp;
	std::cout << "Warn-up queries" << info << "started." << std::endl;
	for (int i = 0; i < warmUpNum; i++) {
		keyReadTemp = fixDigit(keyLen, std::to_string(rand() % rangeSize));
		statusDB = db->Get(ReadOptions(), keyReadTemp, &valueReadTemp);
		if (!statusDB.IsNotFound()) {
			assert(statusDB.ok());  // make sure to check error
		}
	}
	std::cout << "Warn-up queries" << info << "done." << std::endl;
}

// To access members of a structure, use the dot operator
// To access members of a structure through a pointer, use the arrow operator
int main() {
	// initialize the database and the options
	DB* db;
	Options options;
	// disable background & auto compactions
	options.compaction_style = ROCKSDB_NAMESPACE::kCompactionStyleNone;
	options.disable_auto_compactions = true;
	// flushing options, flush the memtable to file
	rocksdb::FlushOptions FlOptions;

	// initialize the timing variables
	clock_t startTime;
	clock_t endTime;

	// optimization
	options.IncreaseParallelism();
	options.OptimizeLevelStyleCompaction();
	options.create_if_missing = true;  // create the DB if it is not already present

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
	bool showPerfStats = false;  // whether or not to show the stats info

	// initialize approximate size info
	std::array<rocksdb::Range, 1> ranges;
  	std::array<uint64_t, 1> sizes;
  	rocksdb::SizeApproximationOptions SAoptions;
  	SAoptions.include_memtables = false;  // include memtable size
  	SAoptions.files_size_error_margin = -1.0;  // error tolerance
	SAoptions.include_files = true;  // include file size

	// whether to warm-up
	bool isWarmUp = false;
	// determine whether or not we are testing "many-small-range" or "a-few-large-range"
	bool isManySmall = false;
	// delete 3 big ranges, with a much higher deletion selectivity
	bool isVeryBig = true;
	std::cout << "isManySmall = " << isManySmall << ", " << "isVeryBig = " << isVeryBig << std::endl;

	// TEST: insert a range of distinct keys
	int rangeSize = 10000;  // the number of key-value pairs to generate
	int numPointQueries = rangeSize/10;  // number of point queries to perform
	std::string dataKey;
	std::string dataValue;
	// assume that each character has size 1 byte, ensure that one key-value pair has 1024 bytes
	int valueLen = 1012;  // the length of the values
	int keyLen = 12;  // the length of each key
	// initialize the containers for the size info
	ranges[0].start = fixDigit(keyLen, std::to_string(0));
	ranges[0].limit = fixDigit(keyLen, std::to_string(rangeSize - 1));
	// generate the workload
	double insertTotalTime = 0.0;  // the total runtime of insertion
	printf("Insertion started.\n");
	for (int i = 0; i < rangeSize; i++) {
		// set up the key
		dataKey = fixDigit(keyLen, std::to_string(i));
		// set up the value, which is a random string
		dataValue = randomString(valueLen);
		// start time of this single operation
		startTime = clock();
		statusDB = db->Put(WriteOptions(), dataKey, dataValue);
		endTime = clock();
		// end time of this single operation
		insertTotalTime += (double)(endTime - startTime) / CLOCKS_PER_SEC;
		assert(statusDB.ok());  // make sure to check error
	}
	printf("Insertion time: %.6fs\n", insertTotalTime);
	std::cout << rangeSize << " key-value pairs inserted." << std::endl;
	
	db->Flush(FlOptions);

	if (showPerfStats) {
		perfContext = *(rocksdb::get_perf_context());
		ioContext = *(rocksdb::get_iostats_context());
		rocksdb::SetPerfLevel(rocksdb::PerfLevel::kDisable);
		std::cout << "=======================================" << std::endl;
		std::cout << perfContext.ToString() << std::endl;
		std::cout << "=======================================" << std::endl;
		std::cout << ioContext.ToString() << std::endl;
	}

  	statusDB = db->GetApproximateSizes(SAoptions, db->DefaultColumnFamily(), ranges.data(), 1, sizes.data());
	assert(statusDB.ok());  // make sure to check error
	std::cout << "Size after insertion: " << sizes[0] << " bytes" << std::endl;

	if (isWarmUp) {
		// perform some warm-up point queries here
		warmUp(statusDB, db, rangeSize, keyLen, numPointQueries/2, "before range deletes");
	}

	// TEST: point read before range deletes, check both present & invalidated keys, do not check non-existing keys
	rocksdb::SetPerfLevel(rocksdb::PerfLevel::kEnableTimeAndCPUTimeExceptForMutex);
	perfContext.Reset();
	ioContext.Reset();
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
	printf("Read throughput before deletes: %.6f entries/s\n", countPointBefore/pointReadTotalTime);
	if (showPerfStats) {
		// output perf context & iostats context results
		perfContext = *(rocksdb::get_perf_context());
		ioContext = *(rocksdb::get_iostats_context());
		rocksdb::SetPerfLevel(rocksdb::PerfLevel::kDisable);
		std::cout << "perf_context before deletes: " << std::endl;
		std::cout << perfContext.ToString() << std::endl;
		std::cout << "iostats_context before deletes: " << std::endl;
		std::cout << ioContext.ToString() << std::endl;
	}
	// obtain the info of size
	statusDB = db->GetApproximateSizes(SAoptions, db->DefaultColumnFamily(), ranges.data(), 1, sizes.data());
	assert(statusDB.ok());  // make sure to check error
	std::cout << "Size after 1st read: " << sizes[0] << " bytes" << std::endl;

	// delete ranges
	Slice startDelete;
	Slice endDelete;
	int rangeDelSize;  // number of elements in each range delete
	int gapSize;  // maintaining a constant-sized gap between the deleted ranges
	int numRangeDel;  // number of range deletes
	int startTemp = rangeSize/100;  // initial starting point of the deletes
	if (isManySmall) {
		rangeDelSize = rangeSize/20;
		gapSize = rangeSize/10;
		numRangeDel = 10;
	}
	else {
		if (isVeryBig) {
			startTemp = rangeSize/10;
			rangeDelSize = rangeSize/4;
			gapSize = 3*rangeSize/10;
			numRangeDel = 3;
		}
		else {
			rangeDelSize = rangeSize/8;
			gapSize = 2*(rangeDelSize + rangeSize/100);
			numRangeDel = 4;
		}
	}
	double rangeDelTotalTime = 0.0;  // total time of the deletes
	// ranges to be deleted: 
	for (int i = 0; i < numRangeDel; i++) {
		// set start (inclusive) and end (inclusive) of the range
		// I thought the end is exclusive, and the documentation implies that it is exclusive, but it is actually inclusive
		// this can be confirmed by doing a full scan after the range deletes
		// some documentations of RocksDB are not up-to-date enough and have typos
		startDelete = fixDigit(keyLen, std::to_string(startTemp));
		endDelete = fixDigit(keyLen, std::to_string(startTemp + rangeDelSize + 1));
		// native range delete, creating a range tombstone
		startTime = clock();  // start time of this operation
		statusDB = db->DeleteRange(WriteOptions(), db->DefaultColumnFamily(), startDelete, endDelete);
		endTime = clock();  // end time of this operation
		assert(statusDB.ok());  // make sure to check error
		rangeDelTotalTime += (double)(endTime - startTime) / CLOCKS_PER_SEC;
		std::cout << "RANGE DELETED [" << startDelete.ToString() << ", " << endDelete.ToString() << ") " << std::endl;
		startTemp += gapSize;
	}

	db->Flush(FlOptions);

	printf("Range deletion time: %.6fs\n", rangeDelTotalTime);
	std::cout << "Number of range deletes: " << numRangeDel << std::endl;
	std::cout << "Size of each range delete: " << rangeDelSize << std::endl;
	// get the size info
	statusDB = db->GetApproximateSizes(SAoptions, db->DefaultColumnFamily(), ranges.data(), 1, sizes.data());
	assert(statusDB.ok());  // make sure to check error
	std::cout << "Size after deletes: " << sizes[0] << " bytes" << std::endl;

	if (isWarmUp) {
		// perform some warm-up point queries here
		warmUp(statusDB, db, rangeSize, keyLen, numPointQueries/2, "after range deletes");
	}

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
	if (showPerfStats) {
		perfContext = *(rocksdb::get_perf_context());
		ioContext = *(rocksdb::get_iostats_context());
		rocksdb::SetPerfLevel(rocksdb::PerfLevel::kDisable);
		std::cout << "=======================================" << std::endl;
		std::cout << perfContext.ToString() << std::endl;
		std::cout << "=======================================" << std::endl;
		std::cout << ioContext.ToString() << std::endl;
	}

	db->Flush(FlOptions);

	statusDB = db->GetApproximateSizes(SAoptions, db->DefaultColumnFamily(), ranges.data(), 1, sizes.data());
	assert(statusDB.ok());  // make sure to check error
	std::cout << "Size after 2nd read: " << sizes[0] << " bytes" << std::endl;
	
	// delete the database and end the test
	delete db;
	return 0;
}
