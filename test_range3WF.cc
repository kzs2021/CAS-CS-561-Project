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
	std::cout << "Warn-up queries" << info << "done. Read " << warmUpNum << " entries." << std::endl;
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
	// whether to flush memtable to file
	bool isFlush = true;

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
	rocksdb::PerfContext perfContext; perfContext.Reset();
	rocksdb::IOStatsContext ioContext; ioContext.Reset();
	bool showPerfStats = false;  // whether or not to show the stats info
	bool showIOStats = true;

	// initialize approximate size info
	std::array<rocksdb::Range, 1> approxSizeRanges;
  	std::array<uint64_t, 1> sizes;
  	rocksdb::SizeApproximationOptions SAoptions;
	SAoptions.include_files = true;  // include file size
  	SAoptions.include_memtables = false;  // include memtable size
  	SAoptions.files_size_error_margin = -1.0;  // error tolerance percentage, -1.0 means exact

	// whether to warm-up
	bool isWarmUpBefore = true;
	bool isWarmUpAfter = true;
	// determine whether or not we are testing "many-small-range" or "a-few-large-range"
	bool isManySmall = false;
	// delete 3 big ranges, with a much higher deletion selectivity
	bool isVeryBig = true;
	// range read or point read
	bool isPointQuery = false;

	// assume that each character has size 1 byte, ensure that one key-value pair has 1024 bytes
	int valueLen = 1012;  // the length of the values
	int keyLen = 12;  // the length of each key
	// workload & query basic config
	int rangeSize = 1000000;  // the number of key-value pairs to generate
	int numPointQueries = rangeSize/10;  // number of point queries to perform
	// the start & end of range queries
	std::string rangeQueryStart = fixDigit(keyLen, std::to_string(rangeSize/4));
	std::string rangeQueryEnd = fixDigit(keyLen, std::to_string(rangeSize/4*3));
	// point query item initialization
	std::string dataKey;  // store the key to be read
	std::string dataValue;  // store the value to be read
	// initialize the containers for the size info
	approxSizeRanges[0].start = fixDigit(keyLen, std::to_string(0));
	approxSizeRanges[0].limit = fixDigit(keyLen, std::to_string(rangeSize - 1));

	// generate the workload, insert a range of distinct keys
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
	// flush the memtable data to file
	if (isFlush) {db->Flush(FlOptions);}
	printf("Insertion time: %.6fs\n", insertTotalTime);
	std::cout << rangeSize << " key-value pairs inserted." << std::endl;
	// obtain the file size
  	statusDB = db->GetApproximateSizes(SAoptions, db->DefaultColumnFamily(), approxSizeRanges.data(), 1, sizes.data());
	assert(statusDB.ok());  // make sure to check error
	std::cout << "Size after insertion: " << sizes[0] << " bytes" << std::endl;

	// perform some warm-up point queries here
	if (isWarmUpBefore) {
		warmUp(statusDB, db, rangeSize, keyLen, numPointQueries/2, " before range deletes ");
	}

	// TEST: point read before range deletes, check both present & invalidated keys, do not check non-existing keys
	std::string keyRead;  // the key which the point query is interested in
	std::string valueRead;  // retrieve the value inserted
	// record the throughput
	double pointThroughPutBefore;
	double rangeThroughPutBefore;
	int countRangeReadBefore;
	rocksdb::SetPerfLevel(rocksdb::PerfLevel::kEnableTimeAndCPUTimeExceptForMutex);
	perfContext.Reset();
	ioContext.Reset();
	if (isPointQuery) {
		// perform some random point queries
		std::set<std::string> keyReadSetBefore;  // ensure that we do not repeatedly visit a key
		int countPointBefore = 0;  // count the number of valid entries retrieved
		double pointReadTotalTimeBefore = 0.0;  // total time of the point queries
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
			pointReadTotalTimeBefore += (double)(endTime - startTime) / CLOCKS_PER_SEC;
			if (!statusDB.IsNotFound()) {countPointBefore++;}
			keyReadSetBefore.insert(keyRead);
		}
		std::cout << "Point read before deletes count: " << countPointBefore << std::endl;
		printf("Point queries runtime before deletes: %.6fs\n", pointReadTotalTimeBefore);
		pointThroughPutBefore = countPointBefore/pointReadTotalTimeBefore;
		printf("Point queries read throughput before deletes: %.6f entries/s\n", pointThroughPutBefore);
	}
	else {  // TEST: range read before deletion
		std::cout << "Range read from " << rangeQueryStart << " to " << rangeQueryEnd << std::endl;
		rocksdb::Iterator* iter = db->NewIterator(rocksdb::ReadOptions());  // the iterator to traverse the data
		countRangeReadBefore = 0;
		double rangeReadTotalTimeBefore = 0.0;  // total time of the range query
		startTime = clock();  // start time of this operation
		for (iter->Seek(rangeQueryStart); iter->Valid() && iter->key().ToString() < rangeQueryEnd; iter->Next()) {
			endTime = clock();
			rangeReadTotalTimeBefore += (double)(endTime - startTime) / CLOCKS_PER_SEC;
			countRangeReadBefore++;  // make sure the time taken for this increment is NOT counted
			startTime = clock();
		}
		endTime = clock();  // end time of this operation
		rangeReadTotalTimeBefore += (double)(endTime - startTime) / CLOCKS_PER_SEC;
		std::cout << "Range read before deletes count: " << countRangeReadBefore << std::endl;
		printf("Range read runtime before deletes: %.6fs\n", rangeReadTotalTimeBefore);
		rangeThroughPutBefore = countRangeReadBefore/rangeReadTotalTimeBefore;
		printf("Range read throughput before deletes: %.6f entries/s\n", rangeThroughPutBefore);
		assert(iter->status().ok()); // check for any errors found during the scan
		delete iter;  // delete the iterator
	}
	// output perf context & iostats context results
	rocksdb::SetPerfLevel(rocksdb::PerfLevel::kDisable);
	if (showPerfStats) {
		perfContext = *(rocksdb::get_perf_context());
		std::cout << "perf_context before deletes: " << std::endl;
		std::cout << perfContext.ToString() << std::endl;
	}
	if (showIOStats) {
		ioContext = *(rocksdb::get_iostats_context());
		std::cout << "iostats_context before deletes: " << std::endl;
		std::cout << ioContext.ToString() << std::endl;
	}

	// implement range deletes
	std::string rangeDeleteStart;
	std::string rangeDeleteEnd;
	int rangeDelSize;  // number of elements in each range delete
	int gapSize;  // maintaining a constant-sized gap between the deleted ranges
	int numRangeDel;  // number of range deletes
	int startTemp = rangeSize/100;  // initial starting point of the deletes
	if (isManySmall) {  // many small-range deletes
		rangeDelSize = rangeSize/20;
		gapSize = rangeSize/10;
		numRangeDel = 10;
	}
	else {
		if (isVeryBig) {  // 3 big-range deletes
			startTemp = rangeSize/10;
			rangeDelSize = rangeSize/4;
			gapSize = 3*rangeSize/10;
			numRangeDel = 3;
		}
		else {  // 4 long-range deletes, but the total number of entries deleted is the same as many small-range deletes
			rangeDelSize = rangeSize/8;
			gapSize = 2*(rangeDelSize + rangeSize/100);
			numRangeDel = 4;
		}
	}
	double rangeDelTotalTime = 0.0;  // total time of the deletes
	// ranges to be deleted
	rocksdb::SetPerfLevel(rocksdb::PerfLevel::kEnableTimeAndCPUTimeExceptForMutex);
	perfContext.Reset();
	ioContext.Reset();
	for (int i = 0; i < numRangeDel; i++) {
		// set start (inclusive) and end (exclusive) of the range
		// this can be confirmed by doing a full scan after the range deletes
		// some documentations of RocksDB are not up-to-date enough and have typos
		rangeDeleteStart = fixDigit(keyLen, std::to_string(startTemp));
		rangeDeleteEnd = fixDigit(keyLen, std::to_string(startTemp + rangeDelSize));
		// native range delete, creating a range tombstone
		startTime = clock();  // start time of this operation
		statusDB = db->DeleteRange(WriteOptions(), db->DefaultColumnFamily(), rangeDeleteStart, rangeDeleteEnd);
		endTime = clock();  // end time of this operation
		assert(statusDB.ok());  // make sure to check error
		rangeDelTotalTime += (double)(endTime - startTime) / CLOCKS_PER_SEC;
		std::cout << "RANGE DELETED [" << rangeDeleteStart << ", " << rangeDeleteEnd << ") " << std::endl;
		startTemp += gapSize;
	}
	// flush the memtable to file
	if (isFlush) {db->Flush(FlOptions);}
	printf("Range deletion time: %.6fs\n", rangeDelTotalTime);
	std::cout << "Number of range deletes: " << numRangeDel << std::endl;
	std::cout << "Number of entries in each range delete: " << rangeDelSize << std::endl;
	// get the size info
	statusDB = db->GetApproximateSizes(SAoptions, db->DefaultColumnFamily(), approxSizeRanges.data(), 1, sizes.data());
	assert(statusDB.ok());  // make sure to check error
	std::cout << "Size after deletes: " << sizes[0] << " bytes" << std::endl;
	// output perf context & iostats context results
	rocksdb::SetPerfLevel(rocksdb::PerfLevel::kDisable);
	if (showPerfStats) {
		perfContext = *(rocksdb::get_perf_context());
		std::cout << "perf_context for range deletes: " << std::endl;
		std::cout << perfContext.ToString() << std::endl;
	}
	if (showIOStats) {
		ioContext = *(rocksdb::get_iostats_context());
		std::cout << "iostats_context for range deletes: " << std::endl;
		std::cout << ioContext.ToString() << std::endl;
	}

	// perform some warm-up point queries here
	if (isWarmUpAfter) {
		warmUp(statusDB, db, rangeSize, keyLen, numPointQueries/2, " after range deletes ");
	}

	rocksdb::SetPerfLevel(rocksdb::PerfLevel::kEnableTimeAndCPUTimeExceptForMutex);
	perfContext.Reset();
	ioContext.Reset();
	if (isPointQuery) {
		// point query after deletion
		std::set<std::string> keyReadSetAfter;  // ensure that we do not repeatedly visit a key
		int countPointValidAfter = 0;  // count the number of valid entries retrieved
		int countPointInvalidAfter = 0;  // count the number of invalid entries retrieved
		double pointReadTotalTimeAfter = 0.0;  // total time of the point queries
		double validPointReadTotalTime = 0.0;  // total time of reading valid entries
		double invalidPointReadTotalTime = 0.0;  // total time of reading invalid entries
		// perform some random point queries after range deletes
		printf("Point read after deletes started.\n");
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
		printf("Point queries runtime after deletes: %.6fs\n", pointReadTotalTimeAfter);
		printf("Time spent for reading valid entries: %.6fs\n", validPointReadTotalTime);
		printf("Time spent for reading invalid entries: %.6fs\n", invalidPointReadTotalTime);
		printf("Point queries average read throughput after deletes: %.6f entries/s\n", numPointQueries/pointReadTotalTimeAfter);
		printf("Point queries read throughput (valid) after deletes: %.6f entries/s\n", countPointValidAfter/validPointReadTotalTime);
		double pointThroughPutAfter = countPointInvalidAfter/invalidPointReadTotalTime;
		printf("Point queries read throughput (invalid) after deletes: %.6f entries/s\n", pointThroughPutAfter);
		printf("Point queries average read throughput drop: %.2f percent\n", (pointThroughPutBefore - pointThroughPutAfter)/pointThroughPutBefore*100.0);
	}
	else {  // range read after deletion
		rocksdb::Iterator* iter = db->NewIterator(rocksdb::ReadOptions());  // the iterator to traverse the data
		int countRangeReadValidAfter = 0;  // count valid keys 
		int countRangeReadInvalidAfter;  // count invalid keys 
		int countRangeReadTotalAfter = countRangeReadBefore;  // count all keys in range
		double rangeReadTotalTimeAfter = 0.0;  // total time of the range query
		startTime = clock();  // start time of this operation
		for (iter->Seek(rangeQueryStart); iter->Valid() && iter->key().ToString() < rangeQueryEnd; iter->Next()) {
			endTime = clock();
			rangeReadTotalTimeAfter += (double)(endTime - startTime) / CLOCKS_PER_SEC;
			countRangeReadValidAfter++;  // make sure the time taken for such increments is NOT counted
			startTime = clock();
		}
		endTime = clock();  // end time of this operation
		rangeReadTotalTimeAfter += (double)(endTime - startTime) / CLOCKS_PER_SEC;
		countRangeReadInvalidAfter = countRangeReadTotalAfter - countRangeReadValidAfter;
		std::cout << "Range read (valid) after deletes count: " << countRangeReadValidAfter << std::endl;
		std::cout << "Range read (invalid) after deletes count: " << countRangeReadInvalidAfter << std::endl;
		std::cout << "Range read (total) after deletes count: " << countRangeReadTotalAfter << std::endl;
		printf("Range read runtime after deletes: %.6fs\n", rangeReadTotalTimeAfter);
		double rangeThroughPutAfter = countRangeReadValidAfter/rangeReadTotalTimeAfter;
		printf("Range read average throughput after deletes: %.6f entries/s\n", rangeThroughPutAfter);
		printf("Range read average read throughput drop: %.2f percent\n", (rangeThroughPutBefore - rangeThroughPutAfter)/rangeThroughPutBefore*100.0);
		assert(iter->status().ok());  // check for any errors found during the scan
		delete iter;  // delete the iterator
	}
	// output perf context & iostats context results
	rocksdb::SetPerfLevel(rocksdb::PerfLevel::kDisable);
	if (showPerfStats) {
		perfContext = *(rocksdb::get_perf_context());
		std::cout << "perf_context after deletes: " << std::endl;
		std::cout << perfContext.ToString() << std::endl;
	}
	if (showIOStats) {
		ioContext = *(rocksdb::get_iostats_context());
		std::cout << "iostats_context after deletes: " << std::endl;
		std::cout << ioContext.ToString() << std::endl;
	}
	
	// delete the database and end the test
	delete db;
	return 0;
}