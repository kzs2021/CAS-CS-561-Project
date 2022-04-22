#include <cstdio>
#include <string>
#include <time.h>

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

	// TEST: insert a range of distinct values
	startTime = clock();  // start time of this operation
	std::string dataKey;
	std::string dataValue;
	for (int i = 1; i <= 10000; i++) {  // test at least 1GB of data
		// insert a series of integers as keys and their tenfold values as values
		dataKey = std::to_string(i);
		dataValue = std::to_string(i * 10);
		statusDB = db->Put(WriteOptions(), dataKey, dataValue);
		assert(statusDB.ok());  // make sure to check error
	}
	endTime = clock();  // end time of this operation
	printf("Insert time: %.2fs\n", (double)(endTime - startTime) / CLOCKS_PER_SEC);
	// delete the database and end the test
	delete db;
	return 0;
}
