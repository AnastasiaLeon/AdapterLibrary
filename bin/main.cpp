#include <iostream>
#include <fstream>
#include <unordered_map>
#include <algorithm>
#include <cctype>
#include <processing.h>

int main(int argc, char** argv) {
    if (argc != 2) return 1;

    std::unordered_map<std::string, size_t> word_counts;
    bool recursive = false;

    std::string all_text;
    for (const auto& file_path : Dir(argv[1], recursive) 
        | Filter([](const auto& p) { return p.extension() == ".txt"; })
        | AsVector()) {
        std::ifstream file(file_path);
        all_text += std::string((std::istreambuf_iterator<char>(file)), 
                               std::istreambuf_iterator<char>());
        all_text += " ";
    }

    auto words = AsDataFlow(std::vector{all_text})
        | Split(" \n\t\r,.!?;:\"")
        | Transform([](std::string word) {
            std::transform(word.begin(), word.end(), word.begin(), 
                [](unsigned char c) { return std::tolower(c); });
            return word;
        })
        | Filter([](const std::string& word) { return !word.empty(); })
        | AsVector();

    for (const auto& word : words) {
        word_counts[word]++;
    }

    for (const auto& [word, count] : word_counts) {
        std::cout << word << " - " << count << "\n";
    }

    return 0;
}