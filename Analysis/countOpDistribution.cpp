#include <csignal>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <regex>
#include <set>
#include <sstream>
#include <unordered_map>
#include <vector>

struct PrefixCategory {
    std::string prefix;
    std::string category;
};

struct OperationStats {
    std::unordered_map<std::string, int> opTypeCount;
};

struct OperationDistribution {
    std::unordered_map<std::string, int> getOpDistributionCount;
    std::unordered_map<std::string, int> updateOpDistributionCount;
    std::unordered_map<std::string, int> deleteOpDistributionCount;
};

std::unordered_map<std::string, OperationStats> stats; // category -> OperationStats
std::unordered_map<std::string, OperationDistribution> opDistribution; // category -> OperationDistribution

std::vector<PrefixCategory> hexPrefixes = {
    { "446174616261736556657273696f6e", "DatabaseVersionKey" },
    { "4c617374486561646572", "HeadHeaderKey" },
    { "4c617374426c6f636b", "HeadBlockKey" },
    { "4c61737446617374", "HeadFastBlockKey" },
    { "4c61737446696e616c697a6564", "HeadFinalizedBlockKey" },
    { "4c61737453746174654944", "PersistentStateIDKey" },
    { "4c6173745069766f74", "LastPivotKey" },
    { "5472696553796e63", "FastTrieProgressKey" },
    { "536e617073686f7444697361626c6564", "SnapshotDisabledKey" },
    { "536e617073686f74526f6f74", "SnapshotRootKey" },
    { "536e617073686f744a6f75726e616c", "SnapshotJournalKey" },
    { "536e617073686f7447656e657261746f72", "SnapshotGeneratorKey" },
    { "536e617073686f745265636f76657279", "SnapshotRecoveryKey" },
    { "536e617073686f7453796e63537461747573", "SnapshotSyncStatusKey" },
    { "536b656c65746f6e53796e63537461747573", "SkeletonSyncStatusKey" },
    { "547269654a6f75726e616c", "TrieJournalKey" },
    { "5472616e73616374696f6e496e6465785461696c", "TxIndexTailKey" },
    { "466173745472616e73616374696f6e4c6f6f6b75704c696d6974", "FastTxLookupLimitKey" },
    { "496e76616c6964426c6f636b", "BadBlockKey" },
    { "756e636c65616e2d73687574646f776e", "UncleanShutdownKey" },
    { "657468322d7472616e736974696f6e", "TransitionStatusKey" },
    { "536e617053796e63537461747573", "SnapSyncStatusFlagKey" },
    { "68", "HeaderPrefix" },
    { "74", "HeaderTDSuffix" },
    { "6e", "HeaderHashSuffix" },
    { "48", "HeaderNumberPrefix" },
    { "62", "BlockBodyPrefix" },
    { "72", "BlockReceiptsPrefix" },
    { "6c", "TxLookupPrefix" },
    { "42", "BloomBitsPrefix" },
    { "61", "SnapshotAccountPrefix" },
    { "6f", "SnapshotStoragePrefix" },
    { "63", "CodePrefix" },
    { "53", "SkeletonHeaderPrefix" },
    { "41", "TrieNodeAccountPrefix" },
    { "4f", "TrieNodeStoragePrefix" },
    { "4c", "StateIDPrefix" },
    { "76", "VerklePrefix" },
    { "7365637572652d6b65792d", "PreimagePrefix" },
    { "657468657265756d2d636f6e6669672d", "ConfigPrefix" },
    { "657468657265756d2d67656e657369732d", "GenesisPrefix" },
    { "6942", "BloomBitsIndexPrefix" },
    { "636874526f6f7456322d", "ChtPrefix" },
    { "6368742d", "ChtTablePrefix" },
    { "636874496e64657856322d", "ChtIndexTablePrefix" },
    { "626c74526f6f742d", "BloomTriePrefix" },
    { "626c742d", "BloomTrieTablePrefix" },
    { "626c74496e6465782d", "BloomTrieIndexPrefix" },
    { "636c697175652d", "CliqueSnapshotPrefix" },
    { "7570646174652d", "BestUpdateKey" },
    { "6669786564526f6f742d", "FixedCommitteeRootKey" },
    { "636f6d6d69747465652d", "SyncCommitteeKey" }
};

std::vector<PrefixCategory> prefixes = {
    { "DatabaseVersion", "DatabaseVersionKey" },
    { "LastHeader", "HeadHeaderKey" },
    { "LastBlock", "HeadBlockKey" },
    { "LastFast", "HeadFastBlockKey" },
    { "LastFinalized", "HeadFinalizedBlockKey" },
    { "LastStateID", "PersistentStateIDKey" },
    { "LastPivot", "LastPivotKey" },
    { "TrieSync", "FastTrieProgressKey" },
    { "SnapshotDisabled", "SnapshotDisabledKey" },
    { "SnapshotRoot", "SnapshotRootKey" },
    { "SnapshotJournal", "SnapshotJournalKey" },
    { "SnapshotGenerator", "SnapshotGeneratorKey" },
    { "SnapshotRecovery", "SnapshotRecoveryKey" },
    { "SnapshotSyncStatus", "SnapshotSyncStatusKey" },
    { "SkeletonSyncStatus", "SkeletonSyncStatusKey" },
    { "TrieJournal", "TrieJournalKey" },
    { "TransactionIndexTail", "TxIndexTailKey" },
    { "FastTransactionLookupLimit", "FastTxLookupLimitKey" },
    { "InvalidBlock", "BadBlockKey" },
    { "unclean-shutdown", "UncleanShutdownKey" },
    { "eth2-transition", "TransitionStatusKey" },
    { "SnapSyncStatus", "SnapSyncStatusFlagKey" },
    { "h", "HeaderPrefix" },
    { "t", "HeaderTDSuffix" },
    { "n", "HeaderHashSuffix" },
    { "H", "HeaderNumberPrefix" },
    { "b", "BlockBodyPrefix" },
    { "r", "BlockReceiptsPrefix" },
    { "l", "TxLookupPrefix" },
    { "B", "BloomBitsPrefix" },
    { "a", "SnapshotAccountPrefix" },
    { "o", "SnapshotStoragePrefix" },
    { "c", "CodePrefix" },
    { "S", "SkeletonHeaderPrefix" },
    { "A", "TrieNodeAccountPrefix" },
    { "O", "TrieNodeStoragePrefix" },
    { "L", "StateIDPrefix" },
    { "v", "VerklePrefix" },
    { "secure-key-", "PreimagePrefix" },
    { "ethereum-config-", "ConfigPrefix" },
    { "ethereum-genesis-", "GenesisPrefix" },
    { "iB", "BloomBitsIndexPrefix" },
    { "chtRootV2-", "ChtPrefix" },
    { "cht-", "ChtTablePrefix" },
    { "chtIndexV2-", "ChtIndexTablePrefix" },
    { "bltRoot-", "BloomTriePrefix" },
    { "blt-", "BloomTrieTablePrefix" },
    { "bltIndex-", "BloomTrieIndexPrefix" },
    { "clique-", "CliqueSnapshotPrefix" },
    { "update-", "BestUpdateKey" },
    { "fixedRoot-", "FixedCommitteeRootKey" },
    { "committee-", "SyncCommitteeKey" },
};

std::set<std::string> targetDistributionCountCategory = {
    "TxLookupPrefix",
    "SnapshotAccountPrefix",
    "SnapshotStoragePrefix",
    "TrieNodeAccountPrefix",
    "TrieNodeStoragePrefix"
};

std::string MatchPrefix(const std::string& key)
{
    for (const auto& prefix : hexPrefixes) {
        if (key.compare(0, prefix.prefix.size(), prefix.prefix) == 0) {
            return prefix.category;
        }
    }
    return "Unknown";
}

bool ParseLogLine(const std::string& line, std::string& opType, std::string& category, std::string& key)
{
    static const std::regex re(R"(OPType: (\w+(?: \w+)*)(?: key: ([a-fA-F0-9]+))?, size: \d+|OPType: (\w+(?: \w+)*))");
    std::smatch matches;

    if (!std::regex_search(line, matches, re)) {
        return false;
    }

    if (matches[1].matched) {
        opType = matches[1];
    } else if (matches[3].matched) {
        opType = matches[3];
    } else {
        return false;
    }

    if (matches[2].matched) {
        key = matches[2];
        category = MatchPrefix(matches[2]);
    } else {
        category = "noPrefix";
    }

    return true;
}

void ProcessLogFile(const std::string& filePath, int progressInterval, std::unordered_map<std::string, OperationStats>& stats)
{
    std::ifstream file(filePath);
    if (!file.is_open()) {
        throw std::runtime_error("Failed to open file: " + filePath);
    }

    std::string line;
    int lineCount = 0;

    while (std::getline(file, line)) {
        lineCount++;

        if (lineCount % progressInterval == 0) {
            std::cout << "\rProcessed " << lineCount << " lines..." << std::flush;
        }

        std::string opType;
        std::string category;
        std::string key;
        if (!ParseLogLine(line, opType, category, key)) {
            std::cerr << "Warning: Failed to parse line: " << line << "\n";
            continue;
        }
        stats[category].opTypeCount[opType]++;

        if (opDistribution.find(category) == opDistribution.end()) {
            opDistribution.emplace(category, OperationDistribution());
        }
        if (opType == "Get") {
            if (opDistribution[category].getOpDistributionCount.find(key) == opDistribution[category].getOpDistributionCount.end()) {
                opDistribution[category].getOpDistributionCount.emplace(key, 1);
            } else {
                opDistribution[category].getOpDistributionCount[key]++;
            }
        } else if (opType == "BatchPut") {
            if (opDistribution[category].updateOpDistributionCount.find(key) == opDistribution[category].updateOpDistributionCount.end()) {
                opDistribution[category].updateOpDistributionCount.emplace(key, 1);
            } else {
                opDistribution[category].updateOpDistributionCount[key]++;
            }
        } else if (opType == "BatchDelete") {
            if (opDistribution[category].deleteOpDistributionCount.find(key) == opDistribution[category].deleteOpDistributionCount.end()) {
                opDistribution[category].deleteOpDistributionCount.emplace(key, 1);
            } else {
                opDistribution[category].deleteOpDistributionCount[key]++;
            }
        }
    }
    file.close();
    std::cout << "\rProcessed a total of " << lineCount << " lines." << std::endl;
}

void PrintStats(const std::unordered_map<std::string, OperationStats>& stats, std::ofstream& outputFile)
{
    outputFile << "Count of KV operations:\n";
    for (const auto& [category, opStats] : stats) {
        outputFile << "Category: " << category << "\n";
        for (const auto& [opType, count] : opStats.opTypeCount) {
            outputFile << "  OPType: " << opType << ", Count: " << count << "\n";
        }
    }
    outputFile << "\n\nDistribution of KV operations:\n";
    for (const auto& [category, opDist] : opDistribution) {
        if (targetDistributionCountCategory.find(category) == targetDistributionCountCategory.end()) {
            continue;
        }
        // sort by count in the map
        std::vector<std::pair<std::string, int>> sortedGetOps(opDist.getOpDistributionCount.begin(),
            opDist.getOpDistributionCount.end());
        std::sort(sortedGetOps.begin(), sortedGetOps.end(),
            [](const std::pair<std::string, int>& a, const std::pair<std::string, int>& b) {
                return a.second > b.second; // Descending order
            });
        uint64_t id = 1;
        std::fstream currentGetFile;
        std::string currentGetFileName = category + "_get_dis.log";
        currentGetFile.open(currentGetFileName, std::ios::out);
        if (!currentGetFile.is_open()) {
            std::cerr << "Error creating output file: " << currentGetFileName << std::endl;
            return;
        }
        currentGetFile << "ID\tCount\n";
        for (const auto& [key, count] : sortedGetOps) {
            currentGetFile << id << "\t" << count << "\n";
            id++;
        }
        currentGetFile.close();
        // sort by count in the map
        std::vector<std::pair<std::string, int>> sortedUpdateOps(opDist.updateOpDistributionCount.begin(),
            opDist.updateOpDistributionCount.end());
        std::sort(sortedUpdateOps.begin(), sortedUpdateOps.end(),
            [](const std::pair<std::string, int>& a, const std::pair<std::string, int>& b) {
                return a.second > b.second; // Descending order
            });

        id = 1;
        std::fstream currentUpdateFile;
        std::string currentUpdateFileName = category + "_update_dis.log";
        currentUpdateFile.open(currentUpdateFileName, std::ios::out);
        if (!currentUpdateFile.is_open()) {
            std::cerr << "Error creating output file: " << currentUpdateFileName << std::endl;
            return;
        }
        currentUpdateFile << "ID\tCount\n";
        for (const auto& [key, count] : sortedUpdateOps) {
            if (count <= 1) {
                break;
            }
            currentUpdateFile << id << "\t" << count << "\n";
            id++;
        }
        currentUpdateFile.close();
        // sort by count in the map
        std::vector<std::pair<std::string, int>> sortedDeleteOps(opDist.deleteOpDistributionCount.begin(),
            opDist.deleteOpDistributionCount.end());
        std::sort(sortedDeleteOps.begin(), sortedDeleteOps.end(),
            [](const std::pair<std::string, int>& a, const std::pair<std::string, int>& b) {
                return a.second > b.second; // Descending order
            });
        id = 1;
        std::fstream currentDeleteFile;
        std::string currentDeleteFileName = category + "_delete_dis.log";
        currentDeleteFile.open(currentDeleteFileName, std::ios::out);
        if (!currentDeleteFile.is_open()) {
            std::cerr << "Error creating output file: " << currentDeleteFileName << std::endl;
            return;
        }
        currentDeleteFile << "ID\tCount\n";
        for (const auto& [key, count] : sortedDeleteOps) {
            currentDeleteFile << id << "\t" << count << "\n";
            id++;
        }
        currentDeleteFile.close();
    }
}

void SignalHandler(int signal)
{
    if (signal == SIGINT) {
        std::ofstream outputFile("operation_count.txt");
        if (outputFile.is_open()) {
            PrintStats(stats, outputFile);
            std::cout << "\nStatistics written to operation_count.txt due to Ctrl+C" << std::endl;
        } else {
            std::cerr << "\nError creating output file during Ctrl+C handling" << std::endl;
        }
        exit(0);
    }
}

int main(int argc, char* argv[])
{
    std::string logFilePath;
    if (argc == 1) {
        logFilePath = "/mnt/sn640/Analysis/block18121461-block18620085-KV-operations.log";
    } else {
        logFilePath = argv[1];
    }

    std::string outputFilePath = "operation_count.txt";
    int progressInterval = 10000;

    // Register signal handler for Ctrl+C
    std::signal(SIGINT, SignalHandler);

    try {
        ProcessLogFile(logFilePath, progressInterval, stats);
    } catch (const std::exception& e) {
        std::cerr << "Error processing log file: " << e.what() << std::endl;
        return 1;
    }

    std::ofstream outputFile(outputFilePath);
    if (!outputFile.is_open()) {
        std::cerr << "Error creating output file: " << outputFilePath << std::endl;
        return 1;
    }

    PrintStats(stats, outputFile);
    std::cout << "Statistics written to " << outputFilePath << std::endl;

    return 0;
}
