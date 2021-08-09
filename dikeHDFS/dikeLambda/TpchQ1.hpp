#ifndef DIKE_TPCH_Q1_HPP
#define DIKE_TPCH_Q1_HPP

#include "DikeAsyncWriter.hpp"
#include "DikeProcessor.hpp"
#include "LambdaBinaryColumnWriter.hpp"
#include "LambdaParquetReader.hpp"

class TpchQ1 : public DikeProcessor {    
    public:
    LambdaParquetReader * lambdaParquetReader = NULL;
    LambdaBinaryColumnWriter * lambdaBinaryColumnWriter = NULL;
    int verbose = 0;
    TpchQ1(){};

    virtual int Run(DikeProcessorConfig & dikeProcessorConfig, DikeIO * output) override;
    virtual void Worker() override;

    int writeColumn(uint8_t * res, int out_rows, int64_t * int64_values);
    int writeColumn(uint8_t * res, int out_rows, double * int64_values);
    int writeColumn(uint8_t * res, int out_rows, parquet::ByteArray * int64_values);
};

#endif /* DIKE_TPCH_Q1_HPP */