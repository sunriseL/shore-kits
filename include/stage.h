/* -*- mode:C++; c-basic-offset:4 -*- */

#ifndef _STAGE_H
#define _STAGE_H

#include "thread.h"
#include "tuple.h"
#include "packet.h"



// include me last!!!
#include "namespace.h"



/* exported datatypes */


class stage_container_t;


/**
 *  @brief A QPIPE stage is a queue of packets (work that must be
 *  completed) and a process_next_packet() function that worker
 *  threads can call to process the packets.
 */
class stage_t {
public:
    struct adaptor_t {
        virtual packet_t *get_packet()=0;
        virtual output_t output(const tuple_t &tuple)=0;
        virtual bool check_for_cancellation()=0;
        virtual void abort_query()=0;
    };

protected:

    // synch vars
    pthread_mutex_t _stage_lock;
    

    char* _stage_name;


    /** The set of packets that this stage is responsible for.
     *
     *  New packets are "merged into the stage" by prepending them to
     *  this list. The tail packet in this list has all of its state
     *  intact, but any packets added in front of it have had their
     *  terminate_inputs() method called. Every packet in this list
     *  will be fed from the input tuple buffer of the tail packet.
     *
     *  The enclosing container may prepend packets to this list, but
     *  only the worker thread responsible for this stage may remove
     *  packets. This cuts down on the amount of synchronization we
     *  must perform.
     *
     *  The enclosing container merges a new packet with this set by
     *  (1) acquiring the _stage_lock, (2) verifying that this stage's
     *  _stage_accepting_packets flag is true, (3) verifying that the
     *  new packet is mergeable with the head of the list, (4)
     *  prepending the new packet to this list, and (5) releasing the
     *  _stage_lock.
     *
     *  A packet's is_mergeable() method should perform read-only
     *  operations on its packet. Furthermore, the answer should not
     *  depend on the current state of this stage. This allows us to
     *  implement output() by simply (1) acquiring the _stage_lock,
     *  (2) acquiring an iterator to this list, (3) releasing the
     *  _stage_lock, and (4) traversing the list and outputting the
     *  tuple.
     */
    packet_list_t* _stage_packets;
    

    /** Whether the enclosing container can still assign packets to
     *  this stage. Can be initialized by the constructor. If this
     *  flag is initialized to true, it should be turned off at some
     *  point in process() (i.e. an aggregate stage would turn this
     *  flag off when it starts outputing results). This flag must be
     *  protected with the _stage_lock.
     */
    bool _stage_accepting_packets;

    
    /** Currently, all known forms of work sharing can be implemented
     *  by tracking the number of output tuples the stage has
     *  produced. We do a slight variation of this here by
     *  initializing the _stage_next_tuple counter to 1 and
     *  incrementing it each time output() is invoked.
     */
    volatile unsigned int _stage_next_tuple;


    /** This stage's container. We need access to this to re-enqueue
     *  unfinished packets and to delete ourselves when we are done.
     */
    stage_container_t* _stage_container;


    /** Whether this stage has been cancelled. The stage can check
     *  this variable at various cancellation points in its process()
     *  and return pre-maturely.
     */
    volatile bool _stage_cancelled;
    
    
    typedef enum {
	OUTPUT_RETURN_CONTINUE = 0,
	OUTPUT_RETURN_STOP,
	OUTPUT_RETURN_ERROR
    } output_t;


    void stop_accepting_packets();


    output_t output(const tuple_t &tuple);


    /** Merge the specified packet into this stage. */
    void merge_packet(packet_t* packet);


    /**
     *  @brief Process this packet. Use output() to output tuples
     *  produced.
     *
     *  @return 0 on success. Non-zero value on unrecoverable
     *  error. If this function returns error, the stage will
     *  terminate all queries that it is currently involved in
     *  computing.
     */
    virtual int process_packet(packet_t* packet)=0;

    
public:

    stage_t();

    virtual ~stage_t();
    
    
    /**
     *  @brief Accessor for this stage's name.
     */
    const char* get_name() const { return _stage_name; }


    void run();


    bool try_merge(packet_t* packet);


    int init_stage(packet_list_t* stage_packets,
		   stage_container_t* stage_container,
		   const char* stage_name);
    
    /**
     *  @brief Every stage must override this method. It should
     *  release stage-level resources, destroy child packets which
     *  have not been dispatched, close (and possibly delete) buffers
     *  from child packets which have been dispatched.
     *
     *  This function should be invoked by the stage's worker
     *  thread. Otherwise, we need to worry about additional
     *  synchronization between the caller and the worker.
     */    
    void finish_stage();
};



#include "namespace.h"
#endif
