#include "stages/aggregate.h"

void aggregate_packet_t::terminate() {
    input_buffer->close();
}

int aggregate_stage_t::process_packet(packet_t *p) {
    aggregate_packet_t *packet = (aggregate_packet_t *)p;

    // automatically close the input buffer when this function exits
    buffer_closer_t input = packet->input_buffer;
    tuple_aggregate_t *aggregate = packet->aggregate;

    // input buffer owns src
    tuple_t src;
    
    // "I" own dest, so allocate space for it on the stack
    size_t dest_size = packet->output_buffer->tuple_size;
    char dest_data[dest_size];
    tuple_t dest(dest_data, dest_size);

    // fire up the input buffer
    input->init_buffer();

    // process normal groups
    while(input->get_tuple(src)) {
        if(aggregate->filter(dest, src)) {
#ifdef BUG_FIXED
            // update mergeable status
            not_mergeable(packet);
#endif

            // output the tuple
            if(output(packet, dest)) 
                return 1;
        }
    }

    // finish off the last group
    aggregate->eof(dest);
    if(output(packet, dest))
        return 1;

    return 0;
}
