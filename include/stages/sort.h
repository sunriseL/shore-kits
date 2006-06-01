// -*- mode:C++; c-basic-offset:4 -*-

#ifndef __SORT_H
#define __SORT_H

#include "stage.h"

using namespace qpipe;

/**
 *@brief Packet definition for the sort stage
 */
struct sort_packet_t : public packet_t {
    tuple_buffer_t *input_buffer;
    tuple_comparator_t *compare;

    sort_packet_t(DbTxn *tid, char *packet_id,
                  tuple_buffer_t *out_buffer,
                  tuple_buffer_t *in_buffer,
                  tuple_filter_t *filt,
                  tuple_comparator_t *cmp)
	: packet_t(tid, packet_id, out_buffer, filt),
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
        : stage_t("SORT_STAGE")
    {
    }

protected:
    virtual int process_packet(packet_t *packet);
};
