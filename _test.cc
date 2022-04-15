#include <cstdio>
#include <string>

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

int main() {
	// initialize the database and the options
	DB* db;
	Options options;
	// Optimize RocksDB. This is the easiest way to get RocksDB to perform well
	options.IncreaseParallelism();
	options.OptimizeLevelStyleCompaction();
	// create the DB if it's not already present
	options.create_if_missing = true;
	// open DB and check the status
	Status statusDB = DB::Open(options, kDBPath, &db);
	assert(statusDB.ok());
	// Put key-value
	statusDB = db->Put(WriteOptions(), "key1", "value");
	assert(statusDB.ok());  // make sure to check error
	// retrieve the value inserted
	std::string value;
	statusDB = db->Get(ReadOptions(), "key1", &value);
	assert(statusDB.ok());  // make sure to check error
	assert(value == "value");  // check that we retrieved the value
	// atomically apply a set of updates
	{
		WriteBatch batch;
		batch.Delete("key1");
		batch.Put("key2", value);
		statusDB = db->Write(WriteOptions(), &batch);
	}
	statusDB = db->Get(ReadOptions(), "key1", &value);
	assert(statusDB.IsNotFound());
	db->Get(ReadOptions(), "key2", &value);
	assert(value == "value");
	{
		PinnableSlice pinnable_val;
		db->Get(ReadOptions(), db->DefaultColumnFamily(), "key2", &pinnable_val);
		assert(pinnable_val == "value");
	}
	// test range deletes #####################################################
	Slice start, end; // set start (inclusive) and end (exclusive) of the range
	// use iterator to delete 
	auto iter = db->NewIterator(ReadOptions());
	for (iter->Seek(start); cmp->Compare(iter->key(), end) < 0; iter->Next()) {
		db->Delete(WriteOptions(), iter->key());
	}
	// native range delete, creating a range tombstone
	db->DeleteRange(WriteOptions(), start, end);
	// test range deletes #####################################################
	{
		std::string string_val;
		// If it cannot pin the value, it copies the value to its internal buffer.
		// The internal buffer could be set during construction.
		PinnableSlice pinnable_val(&string_val);
		db->Get(ReadOptions(), db->DefaultColumnFamily(), "key2", &pinnable_val);
		assert(pinnable_val == "value");
		// If the value is not pinned, the internal buffer must have the value.
		assert(pinnable_val.IsPinned() || string_val == "value");
	}
	PinnableSlice pinnable_val;
	statusDB = db->Get(ReadOptions(), db->DefaultColumnFamily(), "key1", &pinnable_val);
	assert(statusDB.IsNotFound());
	// Reset PinnableSlice after each use and before each reuse
	pinnable_val.Reset();
	db->Get(ReadOptions(), db->DefaultColumnFamily(), "key2", &pinnable_val);
	assert(pinnable_val == "value");
	pinnable_val.Reset();
	// The Slice pointed by pinnable_val is not valid after this point
	// delete the database
	delete db;
	return 0;
}
