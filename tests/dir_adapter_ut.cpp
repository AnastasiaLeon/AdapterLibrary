#include <processing.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <fstream>
#include <filesystem>
#include <set>

namespace fs = std::filesystem;

class DirAdapterTest : public ::testing::Test {
protected:
    static void SetUpTestSuite() {
        fs::create_directories("test_dir/subdir");
        fs::create_directories("test_dir/empty_subdir");
        
        std::ofstream("test_dir/file1.txt").put('a');
        std::ofstream("test_dir/file2.dat").put('a');
        std::ofstream("test_dir/subdir/file3.txt").put('a');
        std::ofstream("test_dir/subdir/file4.log").put('a');
    }

    static void TearDownTestSuite() {
        fs::remove_all("test_dir");
    }

    std::string base_path() const {
        return (fs::current_path() / "test_dir").string();
    }
};

TEST_F(DirAdapterTest, NonRecursiveSearchFindsOnlyTopLevelFiles) {
    auto result = Dir(base_path(), false) | AsVector();
    
    size_t file_count = 0;
    size_t dir_count = 0;
    
    for (const auto& p : result) {
        if (fs::is_directory(p)) {
            dir_count++;
        } else {
            file_count++;
        }
    }
    
    ASSERT_EQ(file_count, 2);
    ASSERT_EQ(dir_count, 2);
}

TEST_F(DirAdapterTest, RecursiveSearchFindsAllFiles) {
    auto result = Dir(base_path(), true) 
        | Filter([](const fs::path& p) { return !fs::is_directory(p); })
        | AsVector();
    
    ASSERT_EQ(result.size(), 4);
    
    std::set<std::string> found_files;
    for (const auto& p : result) {
        found_files.insert(p.filename().string());
    }
    
    ASSERT_TRUE(found_files.count("file1.txt"));
    ASSERT_TRUE(found_files.count("file2.dat"));
    ASSERT_TRUE(found_files.count("file3.txt"));
    ASSERT_TRUE(found_files.count("file4.log"));
}

TEST_F(DirAdapterTest, NonexistentDirectoryReturnsEmpty) {
    auto result = Dir("nonexistent_dir_12345", false) | AsVector();
    ASSERT_TRUE(result.empty());
}

TEST_F(DirAdapterTest, FilterByExtensionWorks) {
    auto result = Dir(base_path(), true)
        | Filter([](const fs::path& p) { 
              return !fs::is_directory(p) && p.extension() == ".txt"; 
          })
        | AsVector();
    
    ASSERT_EQ(result.size(), 2);
    
    std::set<std::string> found_files;
    for (const auto& p : result) {
        ASSERT_EQ(p.extension().string(), ".txt");
        found_files.insert(p.filename().string());
    }
    
    ASSERT_TRUE(found_files.count("file1.txt"));
    ASSERT_TRUE(found_files.count("file3.txt"));
}