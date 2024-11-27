#include "statsDB.h"

StatsDB::StatsDB(string dbPath)
{
    Status s = DB::Open(options, dbPath, &db);
    if (!s.ok()) {
        cerr << "Failed to open database: " << s.ToString() << endl;
        exit(1);
    }
}

StatsDB::~StatsDB()
{
    delete db;
}

bool StatsDB::Put(string key, string value)
{
    Status s = db->Put(writeOptions, key, value);
    return s.ok();
}

bool StatsDB::Get(string key, string& value)
{
    Status s = db->Get(readOptions, key, &value);
    return s.ok();
}

bool StatsDB::Delete(string key)
{
    Status s = db->Delete(writeOptions, key);
    return s.ok();
}
