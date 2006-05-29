// -*- C++ -*-
#ifndef __PACKET_H
#define __PACKET_H

#include "db_cxx.h"
#include "tuple.h"
#include "functors.h"


#include "namespace.h"


/**
 *  @brief A packet in QPIPE is a unit of work that can be processed
 *  by a stage's worker thread.
 */
class packet_t
{

public:

    DbTxn* xact_id;
    char*  packet_id;
    tuple_buffer_t* output_buffer;
    tuple_filter_t* filter;
  
    // mutex to protect the chain of merged packets
    pthread_mutex_t merge_mutex;
    packet_t*       next_merged_packet;
    int             merge_set_size;
  
public:

    // is this packet a candidate for merging?
    bool mergeable;

    virtual ~packet_t(void);

    packet_t::packet_t(DbTxn* tid,
                       char* pack_id,
                       tuple_buffer_t* outbuf,
                       tuple_filter_t* filt,
                       bool mergeable=false);
    /**
     *  @brief Check whether this packet can be merged with the
     *  specified one. By default, packets are not mergeable.
     *
     *  @return false
     */  
    virtual bool is_mergeable(packet_t *) { return false; }

    /**
     * @brief Notifies this packet that it has been terminated. It
     * should cascade the termination to its children, if any, by
     * closing its input buffers.
     */
    virtual void terminate()=0;
    
    virtual void merge(packet_t* packet);

    /**
     *  @brief Packet ID accessor.
     */
    char* get_packet_id(void) { return packet_id; }

};

#include "namespace.h"
#endif
