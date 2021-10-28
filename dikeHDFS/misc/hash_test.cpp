#include <iostream>
#include <string>
#include <unordered_map>
#include <map>
#include <vector>
#include <chrono>

#include "sparsehash/dense_hash_map"

int main(int argc, char** argv)
{
    std::chrono::high_resolution_clock::time_point t1,t2;
    int record_count = 1000000;
    int factor = 4;
    int total_records =  record_count * factor;

    uint64_t * col = new uint64_t[total_records];

    int counter = 0;
    for(int i = 0; i < record_count; i++) {
        for(int j = 0; j < factor; j++){
            col[counter] = i;
            counter++;
        }
    }

    t1 =  std::chrono::high_resolution_clock::now();
    // Calculate hash
    uint64_t hash = 0;
    for(int i = 0; i < total_records; i++){
        hash ^= std::hash<double>{}(*(double *)&col[i]);
    }

    t2 =  std::chrono::high_resolution_clock::now();
    std::chrono::duration<double, std::milli> run_time = t2 - t1;            
    std::cout << "Hash calculation " << run_time.count()/ 1000 << " sec" << std::endl;       

    //std::unordered_map< uint64_t, std::vector<int> > groupingHashMap;
    std::unordered_map< uint64_t, uint64_t > groupingHashMap;
    groupingHashMap.rehash( total_records );
    groupingHashMap.max_load_factor(4);

    t1 =  std::chrono::high_resolution_clock::now();
    int aggregateCount = 0;
    for(int i = 0; i < total_records; i++){
        //hash = std::hash<double>{}(*(double *)&col[i]);
        hash = col[i];
        auto group = groupingHashMap.find(hash);
        if(group != groupingHashMap.end()) { // We found group for our hash
            aggregateCount++;            
        } else { // This hash does not exists            
            groupingHashMap[hash] = i;            
        }        
    }

    t2 =  std::chrono::high_resolution_clock::now();
    run_time = t2 - t1;            
    std::cout << "unordered_map lookup time " << run_time.count()/ 1000 << " sec" << std::endl;

/*
    std::cout << "groupingHashMap.bucket_count() " << groupingHashMap.bucket_count() << std::endl;    
    std::cout << "groupingHashMap.load_factor() " << groupingHashMap.load_factor() << std::endl;
    std::cout << "groupingHashMap.max_load_factor() " << groupingHashMap.max_load_factor() << std::endl;
    std::cout << "groupingHashMap.size() " << groupingHashMap.size() << std::endl;
*/

    google::dense_hash_map< uint64_t, uint64_t > denseHashMap;
    denseHashMap.set_empty_key(-1);
    denseHashMap.set_deleted_key(-2);
    denseHashMap.rehash( total_records );

    t1 =  std::chrono::high_resolution_clock::now();
    aggregateCount = 0;
    for(int i = 0; i < total_records; i++){
        //hash = std::hash<double>{}(*(double *)&col[i]);
        hash = col[i];
        auto group = denseHashMap.find(hash);
        if(group != denseHashMap.end()) { // We found group for our hash
            aggregateCount++;            
        } else { // This hash does not exists            
            denseHashMap[hash] = i;            
        }        
    }

    t2 =  std::chrono::high_resolution_clock::now();
    run_time = t2 - t1;            
    std::cout << "google::dense_hash_map lookup time " << run_time.count()/ 1000 << " sec" << std::endl;



    return 0;
}

// gcc -g -O3 -I ../external/sparsehash-c11 hash_test.cpp -o hash_test -lstdc++
