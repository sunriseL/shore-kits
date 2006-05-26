// -*- C++ -*-
#ifndef __AGGREGATE_H
#define __AGGREGATE_H

#include "tuple.h"
#include "packet.h"
#include "stage.h"



using namespace qpipe;

/**
 * @brief Packet definition for the Aggregation stage
 */
struct aggregate_packet_t : public packet_t {
    tuple_buffer_t *input_buffer;
    tuple_aggregate_t *aggregate;

    aggregate_packet_t(DbTxn *tid, char *packet_id,
                       tuple_buffer_t *out_buffer,
                       tuple_buffer_t *in_buffer,
                       tuple_aggregate_t *agg,
                       tuple_filter_t *filt)
        : packet_t(tid, packet_id, out_buffer, filt),
          input_buffer(in_buffer),
          aggregate(agg)
    {
    }
};

/**
 * @brief Aggregation stage that aggregates over grouped inputs. It
 * produces one output tuple for each input set of tuples.
 */
class aggregate_stage_t : public stage_t {
    void enqueue(packet_t *packet);
    int dequeue();

    aggregate_stage_t(const char *name)
        : stage_t(name)
    {
    }
protected:
    virtual int process_packet(packet_t *packet);
};

#endif
