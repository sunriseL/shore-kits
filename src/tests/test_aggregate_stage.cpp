#include "stages/aggregate.h"

using namespace qpipe;

void *drive_stage(void *arg) {
    stage_t *stage = (stage_t *)arg;
    while(1) {
        stage->process_next_packet();
    }

    return NULL;
}

int main() {
    aggregate_stage_t *stage = new aggregate_stage_t();

    pthread_t stage_thread;
    
        
}
