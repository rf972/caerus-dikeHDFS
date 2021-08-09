#ifndef DIKE_SQL_HPP
#define DIKE_SQL_HPP

#include <sqlite3.h>
#include <iostream>
#include <string.h>
#include <sqlite3.h>

#include "DikeAsyncWriter.hpp"
#include "DikeProcessor.hpp"

class DikeSQL : public DikeProcessor {    
    public:
    sqlite3_stmt * sqlRes;
    DikeSQL(){};

    virtual int Run(DikeProcessorConfig & dikeSQLConfig, DikeIO * output) override;
    virtual void Worker() override;
};

#endif /* DIKE_SQL_HPP */