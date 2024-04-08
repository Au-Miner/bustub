//
// Created by Au_Miner on 2024/4/8.
//

#include "gtest/gtest.h"

namespace bustub {
TEST(CF, CF) {
    std::unordered_map<std::string, int32_t > table_names_;
    table_names_["1"] = 1;
    table_names_["2"] = 2;
    table_names_["3"] = 3;
    auto x = table_names_.find("2");
    std::cout << x->first << " " << x->second << '\n';
}

}
