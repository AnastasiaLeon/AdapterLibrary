#include <processing.h>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

TEST(WriteTest, Write) {
    std::vector<int> input = {1, 2, 3, 4, 5};
    std::stringstream file_emulator;
    auto result = AsDataFlow(input) | Write(file_emulator, '|');
    ASSERT_EQ(file_emulator.str(), "1|2|3|4|5|");
}

TEST(WriteAdapterTest, BasicOutput) {
    std::stringstream ss;
    std::vector<int> data = {1, 2, 3};
    AsDataFlow(data) | Write(ss, ',');
    ASSERT_EQ(ss.str(), "1,2,3,");
}

TEST(WriteTest, EmptyFlow) {
    std::stringstream ss;
    std::vector<int> data;
    AsDataFlow(data) | Write(ss, '|');
    ASSERT_TRUE(ss.str().empty());
}

TEST(WriteTest, CustomDelimiter) {
    std::stringstream ss;
    std::vector<std::string> data = {"a", "b", "c"};
    AsDataFlow(data) | Write(ss, '\n');
    ASSERT_EQ(ss.str(), "a\nb\nc\n");
}