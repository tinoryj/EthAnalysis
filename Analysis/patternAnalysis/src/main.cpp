#include "statsDB.h"
#include <bits/stdc++.h>

using namespace std;

int main(int argc, char* argv[])
{
    string recordsLogPath = argv[1];
    uint64_t targetProcessingNumber = stoull(argv[2]);
    cout << "Processing log file: " << recordsLogPath << ", target process records number: " << targetProcessingNumber << endl;
}