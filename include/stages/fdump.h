// -*- mode:C++; c-basic-offset:4 -*-

#ifndef __FDUMP_H
#define __FDUMP_H

#include "stage.h"



using namespace qpipe;



/* exported constants */

#define FDUMP_STAGE_NAME  "FDUMP_STAGE"
#define FDUMP_PACKET_NAME "FDUMP"



/* exported functions */


/**
 *@brief Packet definition for the sort stage
 */
struct fdump_packet_t : public packet_t {
    tuple_buffer_t *input_buffer;
    char *file_name;

    sort_packet_t(char *packet_id,
                  tuple_buffer_t *out_buffer,
                  tuple_buffer_t *in_buffer,
                  tuple_filter_t *filt,
                  tuple_comparator_t *cmp)
	: packet_t(packet_id, FDUMP_PACKET_NAME, out_buffer, filt),
          input_buffer(in_buffer),
          compare(cmp)
    {
    }

    virtual void terminate();
};



/**
 * @brief Sort stage that partitions the input into sorted runs and
 * merges them into a single output run.
 */

class sort_stage_t : public stage_t {
public:
    sort_stage_t()
        : stage_t(FDUMP_STAGE_NAME)
    {
	// register with the dispatcher
	dispatcher_t::register_stage(FDUMP_PACKET_TYPE, this)
    }

protected:
    virtual int process_packet(packet_t *packet);
};



#endif
