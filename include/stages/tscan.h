// -*- mode:C++ c-basic-offset:4 -*-
#ifndef __TSCAN_H
#define __TSCAN_H

# include "db_cxx.h"
# include "tuple.h"
# include "packet.h"
# include "stage.h"
# include "dispatcher/dispatcher.h"



using namespace qpipe;



/* exported constants */

#define TSCAN_STAGE_NAME  "TSCAN"
#define TSCAN_PACKET_TYPE "TSCAN" 



/* exported functions */


/**
 *@brief Packet definition for the Tscan stage
 */
struct tscan_packet_t : public packet_t {
    Db *input_table;
    bool mergeable;
    tscan_packet_t(char *packet_id,
                   tuple_buffer_t *out_buffer,
                   tuple_filter_t *filt,
                   Db *in_table )
        : packet_t(packet_id, TSCAN_PACKET_TYPE, out_buffer, filt),
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
        : stage_t(TSCAN_STAGE_NAME)
    {
      // register with the dispatcher
      dispatcher_t::register_stage(TSCAN_PACKET_TYPE, this);
    }

protected:
    virtual int process_packet(packet_t *packet);
};



#endif
