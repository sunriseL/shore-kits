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

protected:

    bool merge_enabled;

    virtual bool is_compatible(packet_t*) {
	return false;
    }

public:
    
    char* packet_id;
    char* packet_type;
    
    tuple_buffer_t* output_buffer;
    tuple_filter_t* filter;

    tuple_buffer_t* client_buffer;

    
    /** Should be set to the stage's _stage_next_tuple field when this
	packet is merged into the stage. Should be initialized to 0
	when the packet is first created. */
    unsigned int _stage_next_tuple_on_merge;

    /** Should be set to _stage_next_tuple_on_merge when a packet is
	re-enqued. This lets the stage processing it know to
	send_eof() when its internal _stage_next_tuple counter reaches
	this number. A value of 0 should indicate that the packet must
	receive all tuples produced by the stage. Should be
	initialized to 0. */
    unsigned int _stage_next_tuple_needed;

public:

    packet_t::packet_t(const char* _packet_id,
		       const char* _packet_type,
                       tuple_buffer_t* _output_buffer,
                       tuple_filter_t* _filter,
		       tuple_buffer_t* _client_buffer,
		       bool  _merge_enabled=true);


    virtual ~packet_t(void);


    /**
     *  @brief Check whether this packet can be merged with the
     *  specified one.
     *
     *  @return false
     */  
    
    bool is_merge_enabled() {
	return merge_enabled;
    }

    /**
     *  @brief Check whether this packet can be merged with the
     *  specified one.
     *
     *  @return false
     */  
    
    bool is_mergeable(packet_t* other) {
	return is_merge_enabled() && is_compatible(other);
    }


    void disable_merging() {
	merge_enabled = false;	
    }



    /**
     *  @brief Close input buffers, deleting them if close()
     *  fails. This is invoked when this packet is merged with an
     *  existing packet.
     */
    virtual void terminate_inputs()=0;


    void abort_query();
};



#include "namespace.h"
#endif
