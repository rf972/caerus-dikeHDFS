#ifndef DIKE_BINARY_COLUMN_H
#define DIKE_BINARY_COLUMN_H

#include <stdint.h>
#include <stdlib.h>

#ifdef __cplusplus
extern "C" {
#endif

enum {
    BINARY_COLUMN_BATCH_SIZE = 4096,
    BINARY_COLUMN_TEXT_SIZE = 256 * 1024 - 64, // DikeAsyncWriter::BUFFER_SIZE - 64,
    BINARY_COLUMN_TEXT_MARK = BINARY_COLUMN_TEXT_SIZE - 1024,

};

enum {
    BINARY_COLUMN_TYPE_BOOLEAN = 0,
    BINARY_COLUMN_TYPE_INT32 = 1,
    BINARY_COLUMN_TYPE_INT64 = 2,
    BINARY_COLUMN_TYPE_INT96 = 3,
    BINARY_COLUMN_TYPE_FLOAT = 4,
    BINARY_COLUMN_TYPE_DOUBLE = 5,
    BINARY_COLUMN_TYPE_BYTE_ARRAY = 6,
    BINARY_COLUMN_TYPE_FIXED_LEN_BYTE_ARRAY = 7,
    // Should always be last element.
    BINARY_COLUMN_TYPE_UNDEFINED = 8
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