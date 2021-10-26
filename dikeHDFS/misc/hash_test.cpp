#include <iostream>
#include <string>
#include <unordered_map>

int main(int argc, char** argv)
{ 
    uint64_t h1 = std::hash<std::string>{}("MyString");
    double d = 3.14159;
    uint64_t h2 = std::hash<double>{}(d);
    uint64_t e = 43;
    uint64_t h3 = std::hash<double>{}(*(double *)&e);

    std::cout << std::hex << h1 << " " <<  h2 << " " << h3 << std::endl;

    return 0;
}

// gcc -g -O0 hash_test.cpp -o hash_test -lstdc++
