#ifndef DIKE_BINARY_COLUMN_H
#define DIKE_BINARY_COLUMN_H

#include <stdint.h>
#include <stdlib.h>
#include "sqlite3.h"

#ifdef __cplusplus
extern "C" {
#endif

enum {
    BINARY_COLUMN_BATCH_SIZE = 4096,
    BINARY_COLUMN_TEXT_SIZE = 256 * 1024 - 64, // DikeAsyncWriter::BUFFER_SIZE - 64,
    BINARY_COLUMN_TEXT_MARK = BINARY_COLUMN_TEXT_SIZE - 1024,

};

typedef struct DikeBinaryColumn {   
    int data_type;

    uint8_t * start_pos;
    uint8_t * pos;
    uint8_t * end_pos;

    uint8_t * start_idx;
    uint8_t * idx_pos;    
} DikeBinaryColumn_t;


void DikeBinaryColumnInit(DikeBinaryColumn_t * p, int data_type);
void DikeBinaryColumnDestroy(DikeBinaryColumn_t * p);


#ifdef __cplusplus
}
#endif

#endif /*  DIKE_BINARY_COLUMN_H */