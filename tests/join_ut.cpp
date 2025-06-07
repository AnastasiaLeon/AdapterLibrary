#include <processing.h>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

struct Student {
    uint64_t group_id;
    std::string name;

    bool operator==(const Student& other) const = default;
};

struct Group {
    uint64_t id;
    std::string name;

    bool operator==(const Group& other) const = default;
};

TEST(SimpleTest, JoinKV) {
    std::vector<KV<int, std::string>> left = {{0, "a"}, {1, "b"}, {2, "c"}, {3, "d"}, {1, "e"}};
    std::vector<KV<int, std::string>> right = {{0, "f"}, {1, "g"}, {3, "i"}};

    auto left_flow = AsDataFlow(left);
    auto right_flow = AsDataFlow(right);
    auto result = left_flow | Join(right_flow) | AsVector();

    ASSERT_THAT(
        result,
        testing::ElementsAre(
            JoinResult<std::string, std::string>{"a", "f"},
            JoinResult<std::string, std::string>{"b", "g"},
            JoinResult<std::string, std::string>{"c", std::nullopt},
            JoinResult<std::string, std::string>{"d", "i"},
            JoinResult<std::string, std::string>{"e", "g"}
        )
    );
}

TEST(SimpleTest, JoinComparators) {
    std::vector<Student> students = {{0, "a"}, {1, "b"}, {2, "c"}, {3, "d"}, {1, "e"}};
    std::vector<Group> groups = {{0, "f"}, {1, "g"}, {3, "i"}};

    auto students_flow = AsDataFlow(students);
    auto groups_flow = AsDataFlow(groups);

    auto result =
        students_flow
            | Join(
                groups_flow,
                [](const Student& student) { return student.group_id; },
                [](const Group& group) { return group.id; })
            | AsVector()
    ;

    ASSERT_THAT(
        result,
        testing::ElementsAre(
            JoinResult<Student, Group>{Student{0, "a"}, Group{0, "f"}},
            JoinResult<Student, Group>{Student{1, "b"}, Group{1, "g"}},
            JoinResult<Student, Group>{Student{2, "c"}, std::nullopt},
            JoinResult<Student, Group>{Student{3, "d"}, Group{3, "i"}},
            JoinResult<Student, Group>{Student{1, "e"}, Group{1, "g"}}
        )
    );
}

TEST(JoinCustomTypesTest, JoinWithoutKV) {
    struct Department {
        int id;
        std::string name;
        
        bool operator==(const Department& other) const {
            return id == other.id && name == other.name;
        }
    };
    
    struct Employee {
        int dept_id;
        std::string name;

        bool operator==(const Employee& other) const {
            return dept_id == other.dept_id && name == other.name;
        }
    };
    
    std::vector<Department> depts = {{1, "HR"}, {2, "IT"}};
    std::vector<Employee> emps = {{1, "Alice"}, {1, "Bob"}, {3, "Charlie"}};
    
    auto result = AsDataFlow(emps)
        | Join(
            AsDataFlow(depts),
            [](const Employee& e) { return e.dept_id; },
            [](const Department& d) { return d.id; }
        )
        | AsVector();
    
    ASSERT_EQ(result.size(), 3);
    EXPECT_EQ(result[0].base.dept_id, 1);
    EXPECT_EQ(result[0].base.name, "Alice");
    EXPECT_TRUE(result[0].joined.has_value());
    EXPECT_EQ(result[0].joined->id, 1);
    EXPECT_EQ(result[0].joined->name, "HR");
    
    EXPECT_EQ(result[1].base.dept_id, 1);
    EXPECT_EQ(result[1].base.name, "Bob");
    EXPECT_TRUE(result[1].joined.has_value());
    EXPECT_EQ(result[1].joined->id, 1);
    EXPECT_EQ(result[1].joined->name, "HR");
    
    EXPECT_EQ(result[2].base.dept_id, 3);
    EXPECT_EQ(result[2].base.name, "Charlie");
    EXPECT_FALSE(result[2].joined.has_value());
}

TEST(JoinCustomTypesTest, MultipleMatches) {
    struct Order {
        int customer_id;
        std::string item;

        bool operator==(const Order& other) const {
            return customer_id == other.customer_id && item == other.item;
        }
    };
    
    std::vector<KV<int, std::string>> customers = {{1, "Alice"}, {2, "Bob"}};
    std::vector<Order> orders = {{1, "Book"}, {1, "Pen"}, {3, "Globe"}};
    
    auto result = AsDataFlow(orders)
        | Join(
            AsDataFlow(customers),
            [](const Order& o) { return o.customer_id; },
            [](const KV<int, std::string>& c) { return c.key; }
        )
        | AsVector();
    
    ASSERT_EQ(result.size(), 3);
    
    EXPECT_EQ(result[0].base.customer_id, 1);
    EXPECT_EQ(result[0].base.item, "Book");
    EXPECT_TRUE(result[0].joined.has_value());
    EXPECT_EQ(result[0].joined->key, 1);
    EXPECT_EQ(result[0].joined->value, "Alice");
    
    EXPECT_EQ(result[1].base.customer_id, 1);
    EXPECT_EQ(result[1].base.item, "Pen");
    EXPECT_TRUE(result[1].joined.has_value());
    EXPECT_EQ(result[1].joined->key, 1);
    EXPECT_EQ(result[1].joined->value, "Alice");
    
    EXPECT_EQ(result[2].base.customer_id, 3);
    EXPECT_EQ(result[2].base.item, "Globe");
    EXPECT_FALSE(result[2].joined.has_value());
}