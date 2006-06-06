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
    static const char *PACKET_TYPE;
    typedef vector<tuple_buffer_t*> buffer_list_t;
    
    buffer_list_t input_buffers;
    size_t merge_factor;
    tuple_comparator_t* comparator;

    merge_packet_t(char *packet_id,
                  tuple_buffer_t *out_buffer,
                   tuple_buffer_t *client_buffer,
                  const buffer_list_t &in_buffers,
                  tuple_filter_t *filt,
                  size_t factor,
                   tuple_comparator_t *cmp)
	: packet_t(packet_id, PACKET_TYPE, out_buffer, filt, client_buffer),
          input_buffers(in_buffers),
          merge_factor(factor), comparator(cmp)
    {
    }

    virtual void terminate_inputs();
};



/**
 * @brief Merge stage that merges N sorted inputs into one sorted
 * output run.
 */
class merge_stage_t : public stage_t {
public:
    static const char *DEFAULT_STAGE_NAME;
    
protected:
    virtual int process_packet(packet_t *packet);
};



#endif
