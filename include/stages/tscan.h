// -*- mode:C++ c-basic-offset:4 -*-
#ifndef __TSCAN_H
#define __TSCAN_H

#include "db_cxx.h"
#include "tuple.h"
#include "packet.h"
#include "stage.h"

using namespace qpipe;

/**
 *@brief Packet definition for the Tscan stage
 */
struct tscan_packet_t : public packet_t {
    Db *input_table;
    bool mergeable;
    tscan_packet_t(DbTxn *tid, char *packet_id,
                   tuple_buffer_t *out_buffer,
                   tuple_filter_t *filt,
                   Db *in_table )
        : packet_t(tid, packet_id, out_buffer, filt),
          input_table(in_table),
          mergeable(true)
    {
    }

    virtual void terminate();
};

/**
 *  @brief Table scan stage that reads tuples from the storage manager.
 */

class tscan_stage_t : public stage_t {
public:
    tscan_stage_t()
        : stage_t("tscan stage")
    {
    }
protected:
    virtual int process_packet(packet_t *packet);
};

#endif
