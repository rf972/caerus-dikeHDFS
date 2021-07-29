#include "DikeBinaryColumn.h"

void DikeBinaryColumnInit(DikeBinaryColumn_t * p, int data_type) {
    p->start_pos = 0;
    p->pos = 0;
    p->end_pos = 0;

    p->start_idx = 0;
    p->idx_pos = 0;    

    p->data_type = data_type;
    switch(data_type){
        case SQLITE_INTEGER:
        case SQLITE_FLOAT: // We transfering 8 bytes in Big Endian
            p->start_pos =  (uint8_t *) malloc(BINARY_COLUMN_BATCH_SIZE * sizeof(int64_t));
            p->pos = p->start_pos;
            p->end_pos = p->pos + BINARY_COLUMN_BATCH_SIZE * sizeof(int64_t);
        break;
        
        case SQLITE3_TEXT:
            p->start_pos = (uint8_t *) malloc(BINARY_COLUMN_TEXT_SIZE); 
            p->pos = p->start_pos;
            p->end_pos = p->pos + BINARY_COLUMN_TEXT_SIZE;
            p->start_idx = (uint8_t *) malloc(BINARY_COLUMN_BATCH_SIZE);
            p->idx_pos = p->start_idx;
        break;
    }
}

void DikeBinaryColumnDestroy(DikeBinaryColumn_t * p) {
    free(p->start_pos);
    if(p->start_idx) {
        free(p->start_idx);
    }    
}
