#ifndef DIKE_SQL_HPP
#define DIKE_SQL_HPP

#include <sqlite3.h>
#include <iostream>
#include <string.h>


struct DikeSQLParam {    
    std::string query;
    std::string schema;    
};

class DikeSQL {
    public:
    static int Run(std::istream& inStream, std::ostream& outStream, DikeSQLParam * dikeSQLParam);
};

#endif /* DIKE_SQL_HPP */