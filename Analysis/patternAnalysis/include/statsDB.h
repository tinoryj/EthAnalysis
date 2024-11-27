#include <bits/stdc++.h>
#include <rocksdb/db.h>
#include <rocksdb/options.h>
#include <rocksdb/slice.h>

using namespace rocksdb;
using namespace std;

class StatsDB {
private:
    DB* db;
    Options options;
    WriteOptions writeOptions;
    ReadOptions readOptions;

public:
    StatsDB(string dbPath);
    ~StatsDB();
    bool Put(string key, string value);
    bool Get(string key, string& value);
    bool Delete(string key);
};
