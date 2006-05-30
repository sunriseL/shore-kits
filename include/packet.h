// -*- mode:C++ c-basic-offset:4 -*-
#ifndef __PACKET_H
#define __PACKET_H

#include "db_cxx.h"
#include "tuple.h"
#include "functors.h"
#include <list>


#include "namespace.h"

using std::list;
class packet_t;
typedef list<packet_t*> packet_list_t;

/**
 *  @brief A packet in QPIPE is a unit of work that can be processed
 *  by a stage's worker thread.
 *
 *  After a packet it created, it is given to the dispatcher, which
 *  inserts the packet into some stage's work queue. The packet is
 *  eventually dequeued by a worker thread for that stage, inserted
 *  into that stage's working set, and processed.
 *
 *  Before the dispatcher inserts the next packet into a stage's work
 *  queue, it will check the working set to see if the new packet can
 *  be merged with an existing one that is already being processed.
 */
class packet_t
{

public:

    DbTxn* xact_id;

    // we will index all packets by a unique string ID
    char*  packet_id;

    tuple_buffer_t* output_buffer;
    tuple_filter_t* filter;
  
    // mutex to protect the chain of merged packets
    pthread_mutex_t merge_mutex;
    packet_list_t merged_packets;
  
    // is this packet a candidate for merging?
    bool mergeable;

public:

    virtual ~packet_t(void);

    packet_t::packet_t(DbTxn* tid,
                       char* pack_id,
                       tuple_buffer_t* outbuf,
                       tuple_filter_t* filt,
                       bool mergeable=false);

    /**
     *  @brief Packet ID accessor.
     */
    char* get_packet_id(void) { return packet_id; }

    /**
     *  @brief Check whether this packet can be merged with the
     *  specified one. By default, packets are not mergeable.
     *
     *  @return false
     */  
    virtual bool is_mergeable(packet_t *) { return false; }


    /**
     *  @brief Merge the specified packet with this one.
     */
    virtual void merge(packet_t* packet);


    /**
     * @brief Notifies this packet that it has been terminated. It
     * should cascade the termination to its children, if any, by
     * closing its input buffers.
     */
    virtual void terminate()=0;
};

#include "namespace.h"
#endif
