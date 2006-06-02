/* -*- mode:C++; c-basic-offset:4 -*- */

#ifndef __PACKET_H
#define __PACKET_H

#include "db_cxx.h"
#include "tuple.h"
#include "functors.h"
#include <list>



// include me last!!!
#include "namespace.h"

using std::list;



/* exported datatypes */

class   packet_t;
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
    

    
    char*  packet_id;
    char*  packet_type;

    tuple_buffer_t* output_buffer;
    tuple_filter_t* filter;
  
    // is this packet a candidate for merging?
    bool  mergeable;    

    // mutex to protect the chain of merged packets
    pthread_mutex_t merge_mutex;
    packet_list_t   merged_packets;
  

public:

    packet_t::packet_t(char* _packet_id,
		       char* _packet_type,
                       tuple_buffer_t* _output_buffer,
                       tuple_filter_t* _filter,
                       bool _mergeable=false);

    virtual ~packet_t(void);


    /**
     *  @brief Check whether this packet can be merged with the
     *  specified one. By default, two packets are not mergeable.
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
