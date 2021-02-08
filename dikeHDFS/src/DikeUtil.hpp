#ifndef DIKE_UTIL_HPP
#define DIKE_UTIL_HPP

#include <iostream>
#include <chrono> 
#include <ctime>
#include <string>
#include <sstream> 
#include <iomanip>
#include <thread>
#include <queue>
#include <mutex>
#include <cassert>
#include <semaphore.h>
#include <unistd.h>

class DikeUtil{
    public:
    DikeUtil(){ }
    ~DikeUtil(){ }

    std::string Now(){
        std::ostringstream now;
        
        time_t curr_time;
	    curr_time = time(NULL);
	    tm *tm_local = localtime(&curr_time);
        now << std::to_string(tm_local->tm_year - 100) << "/";
        now << std::setw(2) << std::setfill('0') << std::to_string(tm_local->tm_mon + 1) << "/";
        now << std::setw(2) << std::setfill('0') << std::to_string(tm_local->tm_mday) << " ";
        now << std::setw(2) << std::setfill('0') << std::to_string(tm_local->tm_hour) << ":";
        now << std::setw(2) << std::setfill('0') << std::to_string(tm_local->tm_min)  << ":";
        now << std::setw(2) << std::setfill('0') << std::to_string(tm_local->tm_sec);         
        return now.str();
    }
    
    std::string Reset() {
        std::string Reset("\033[0m");
        return Reset;
    }
    std::string Red() {
        std::string Red("\033[0;31m"); 
        return Red;
     }
    std::string Green() {
        std::string Green("\033[0;32m");
        return Green;
    }
    std::string Yellow() {
        std::string Yellow("\033[0;33m");
        return Yellow;
    }
    std::string Blue() {
        std::string Blue("\033[0;34m");
        return Blue;
    }
    std::string Purple() {
        std::string Purple("\033[0;35m");
        return Purple;
    } 
};

#endif /* DIKE_UTIL_HPP */