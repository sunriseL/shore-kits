#include "stages/aggregate.h"

int aggregate_stage_t::process_packet(packet_t *p) {
    aggregate_packet_t *packet = (aggregate_packet_t *)p;

    // automatically close the input buffer when this function exits
    buffer_closer_t input = packet->input_buffer;
    
    tuple_aggregate_t *aggregate = packet->aggregate;

    tuple_t src;
    tuple_t dest;

    // process normal groups
    while(input->get_tuple(src)) {
        if(aggregate->filter(dest, src))
            if(output(packet, dest)) 
                return 1;
    }

    // finish off the last group
    aggregate->eof(dest);
    if(output(packet, dest))
        return 1;

    return 0;
}
