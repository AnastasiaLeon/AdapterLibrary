#include <processing.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <fstream>
#include <filesystem>
#include <sstream>

namespace fs = std::filesystem;

class OpenFilesTest : public ::testing::Test {
protected:
    static void SetUpTestSuite() {
        fs::create_directories("test_dir");
        std::ofstream("test_dir/file1.txt") << "content1";
        std::ofstream("test_dir/file2.txt") << "content2";
    }

    static void TearDownTestSuite() {
        fs::remove_all("test_dir");
    }
};

TEST_F(OpenFilesTest, OpensExistingFiles) {
    auto paths = Dir("test_dir") 
        | Filter([](const fs::path& p) { return p.extension() == ".txt"; })
        | AsVector();

    size_t count = 0;
    auto flow = AsDataFlow(paths) | OpenFiles();
    for (const auto& file : flow) {
        EXPECT_TRUE(file.is_open());
        count++;
    }
    EXPECT_EQ(count, 2);
}

TEST_F(OpenFilesTest, ReadsFileContentsCorrectly) {
    auto paths = Dir("test_dir")
        | Filter([](const fs::path& p) { return p.filename() == "file1.txt"; })
        | AsVector();

    std::string content;
    auto flow = AsDataFlow(paths) | OpenFiles();
    for (auto& file : flow) {
        std::getline(file, content);
    }
    EXPECT_EQ(content, "content1");
}

TEST_F(OpenFilesTest, EmptyInputProducesEmptyOutput) {
    size_t count = 0;
    auto flow = AsDataFlow(std::vector<fs::path>{}) | OpenFiles();
    for (const auto& file : flow) {
        count++;
    }
    EXPECT_EQ(count, 0);
}

TEST_F(OpenFilesTest, NonExistentFiles) {
    auto paths = std::vector{fs::path("nonexistent_file.txt")};
    
    size_t count = 0;
    auto flow = AsDataFlow(paths) | OpenFiles();
    for (const auto& file : flow) {
        EXPECT_FALSE(file.is_open());
        count++;
    }
    EXPECT_EQ(count, 1);
}