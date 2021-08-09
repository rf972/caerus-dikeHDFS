#ifndef LAMBDA_BINARY_COLUMN_WRITER_HPP
#define LAMBDA_BINARY_COLUMN_WRITER_HPP

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

#include "DikeUtil.hpp"
#include "DikeIO.hpp"
#include "DikeBuffer.hpp"
#include "DikeAsyncWriter.hpp"

#include "DikeBinaryColumn.h"

class LambdaBinaryColumnWriter : public DikeAsyncWriter {
    public:
    LambdaBinaryColumnWriter(DikeIO * output) : DikeAsyncWriter(output) { }
};

#endif /* LAMBDA_BINARY_COLUMN_WRITER_HPP */