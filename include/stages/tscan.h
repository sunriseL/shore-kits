// -*- mode:C++ c-basic-offset:4 -*-
#ifndef __FSCAN_H
#define __FSCAN_H

#include "db_cxx.h"
#include "tuple.h"
#include "packet.h"
#include "stage.h"

using namespace qpipe;

/**
 *@brief Packet definition for the FScan stage
 */
struct fscan_packet_t : public packet_t {
    Db *input_table;
    bool mergeable;
    fscan_packet_t(DbTxn *tid, char *packet_id,
                   tuple_buffer_t *out_buffer,
                   tuple_filter_t *filt,
                   Db *in_table
                   )
        : packet_t(tid, packet_id, out_buffer, filt),
          input_table(in_table),
          mergeable(true)
    {
    }

    virtual void terminate();
};

/**
 *@brief File scan stage that reads tuples from the storage manager.
 */
class fscan_stage_t : public stage_t {
public:
    fscan_stage_t()
        : stage_t("fscan stage")
    {
    }
protected:
    virtual int process_packet(packet_t *packet);
};

#endif
