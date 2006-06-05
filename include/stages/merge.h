// -*- mode:C++; c-basic-offset:4 -*-

#ifndef __MERGE_H
#define __MERGE_H

#include "stage.h"
#include "dispatcher.h"

#include <vector>

using std::vector;


using namespace qpipe;

/**
 *@brief Packet definition for an N-way merge stage
 */
struct merge_packet_t : public packet_t {
    static const char *TYPE;
    typedef vector<tuple_buffer_t*> buffer_list_t;
    
    buffer_list_t input_buffers;
    size_t merge_factor;
    tuple_comparator_t* comparator;

    merge_packet_t(char *packet_id,
                  tuple_buffer_t *out_buffer,
                  const buffer_list_t &in_buffers,
                  tuple_filter_t *filt,
                  size_t factor,
                   tuple_comparator_t *cmp)
	: packet_t(packet_id, TYPE, out_buffer, filt),
          input_buffers(in_buffers),
          merge_factor(factor), comparator(cmp)
    {
    }

    virtual void terminate();
};



/**
 * @brief Merge stage that merges N sorted inputs into one sorted
 * output run.
 */
class merge_stage_t : public stage_t {
public:
    static const char *DEFAULT_STAGE_NAME;
    
    merge_stage_t(packet_list_t *packets,
                 stage_container_t *container,
                 const char *name)
        : stage_t(packets, container, name, true)
    {
    }


protected:
    virtual int process_packet(packet_t *packet);
};



#endif
