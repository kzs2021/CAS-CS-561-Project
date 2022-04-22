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
	// Optimize RocksDB. This is the easiest way to get RocksDB to perform well
	options.IncreaseParallelism();
	options.OptimizeLevelStyleCompaction();
	options.create_if_missing = true;  // create the DB if it's not already present
	options.error_if_exists = true;  // raise an error if the DB already exists
	// open DB and check the status
	Status statusDB = DB::Open(options, kDBPath, &db);
	assert(statusDB.ok());  // make sure to check error

	// TEST: insert a range of distinct values =======================================
	startTime = clock();  // start time of this operation
	std::string dataKey;
	std::string dataValue;
	for (int i = 1; i <= 10; i++) {
		// insert a series of integers as keys and their tenfold values as values
		dataKey = std::to_string(i);
		dataValue = std::to_string(i * 10);
		statusDB = db->Put(WriteOptions(), dataKey, dataValue);
		assert(statusDB.ok());  // make sure to check error
	}
	endTime = clock();  // end time of this operation
	printf("Insert time: %.2fs\n", (double)(endTime - startTime) / CLOCKS_PER_SEC);
	// ===============================================================================

	// non-empty point and range look-ups ============================================
	std::string valueRead;  // retrieve the value inserted, check what "1" corresponds to
	statusDB = db->Get(ReadOptions(), "1", &valueRead);
	assert(statusDB.ok());  // make sure to check error
	assert(valueRead == "10");  // check that we retrieved the value
	// range scan
	rocksdb::Iterator* iter = db->NewIterator(rocksdb::ReadOptions());
	// traverse all data
	for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
		cout << "FULL" << iter->key().ToString() << ": " << iter->value().ToString() << endl;
	}
	assert(iter->status().ok()); // Check for any errors found during the scan
	// traverse a range
	std::string rangeReadStart;
	std::string rangeReadEnd;
	for (iter->Seek(rangeReadStart); iter->Valid() && iter->key().ToString() < rangeReadEnd; iter->Next()) {
		cout << "RANGE [" << rangeReadStart << ", " << rangeReadEnd << "] " << iter->key().ToString() << ": " 
			 << iter->value().ToString() << endl;
	}
	assert(iter->status().ok()); // Check for any errors found during the scan
	delete iter;
	// ===============================================================================

	// batched operations, test single delete ========================================
	std::string valueDel;
	{  // batch is used for ensuring atomicity
		WriteBatch batch;  // initialize the batch
		batch.Delete("1");  // add new operation(s) to the batch
		// ensure that SingleDelete only applies to a key having not been deleted using Delete()
		batch.SingleDelete("2");
		statusDB = db->Write(WriteOptions(), &batch);  // apply the batched operations to the DB
	}
	statusDB = db->Get(ReadOptions(), "1", &valueDel);
	assert(statusDB.IsNotFound());  // check that the value has been deleted
	// ===============================================================================

	// test read pinnacle slice ===========================================================
	{
		PinnableSlice pinnable_val;
		db->Get(ReadOptions(), db->DefaultColumnFamily(), "3", &pinnable_val);
		assert(pinnable_val == "30");
	}
	// ===============================================================================

	// comparing the performance of many small range deletes & a few long range deletes
	// test range deletes ============================================================
	Slice startDelete; // set start (inclusive) and end (exclusive) of the range
	Slice endDelete;
	startTime = clock();
	// native range delete, creating a range tombstone
	db->DeleteRange(WriteOptions(), startDelete, endDelete);
	endTime = clock();
	printf("Range delete time: %.2fs\n", (double)(endTime - startTime) / CLOCKS_PER_SEC);
	// ===============================================================================

	// delete the database and end the test
	delete db;
	return 0;
}
