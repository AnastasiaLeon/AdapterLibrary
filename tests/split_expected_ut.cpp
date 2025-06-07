#include <processing.h>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <expected>

struct Department {
    std::string name;
    
    bool operator==(const Department& other) const {
        return name == other.name;
    }
};

std::expected<Department, std::string> ParseDepartment(const std::string& str) {
    if (str.empty()) {
        return std::unexpected("Department name is empty");
    }
    if (str.contains(' ')) {
        return std::unexpected("Department name contains space");
    }
    return Department{str};
}

TEST(SplitExpectedTest, SplitExpected) {
    std::vector<std::stringstream> files(1);
    files[0] << "good-department|bad department||another-good-department";

    auto [unexpected_flow, good_flow] = AsDataFlow(files) | Split("|") | Transform(ParseDepartment) | SplitExpected(ParseDepartment);

    std::stringstream unexpected_file;
    unexpected_flow | Write(unexpected_file, '.');

    auto expected_result = good_flow | AsVector();

    ASSERT_EQ(unexpected_file.str(), "Department name contains space.Department name is empty.");
    ASSERT_THAT(expected_result, testing::ElementsAre(Department{"good-department"}, Department{"another-good-department"}));
}

TEST(SplitExpectedBaseTest, SplitAlreadyExpected) {
    std::vector<std::expected<int, std::string>> input = {
        std::expected<int, std::string>{1},
        std::unexpected("error1"),
        std::expected<int, std::string>{2}
    };
    
    auto [errors, values] = AsDataFlow(input) | SplitExpected();
    
    ASSERT_THAT(values | AsVector(), testing::ElementsAre(1, 2));
    ASSERT_THAT(errors | AsVector(), testing::ElementsAre("error1"));
}

TEST(SplitExpectedBaseTest, AllGoodValues) {
    std::vector<std::expected<int, std::string>> input = {
        std::expected<int, std::string>{1},
        std::expected<int, std::string>{2}
    };
    
    auto [errors, values] = AsDataFlow(input) | SplitExpected();
    
    ASSERT_TRUE((errors | AsVector()).empty());
    ASSERT_THAT(values | AsVector(), testing::ElementsAre(1, 2));
}

TEST(SplitExpectedBaseTest, AllErrors) {
    std::vector<std::expected<int, std::string>> input = {
        std::unexpected("error1"),
        std::unexpected("error2")
    };
    
    auto [errors, values] = AsDataFlow(input) | SplitExpected();
    
    ASSERT_TRUE((values | AsVector()).empty());
    ASSERT_THAT(errors | AsVector(), testing::ElementsAre("error1", "error2"));
}