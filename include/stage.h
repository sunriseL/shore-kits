/* -*- mode:C++; c-basic-offset:4 -*- */

#ifndef _STAGE_H
#define _STAGE_H

#include "tuple.h"
#include "packet.h"



// include me last!!!
#include "namespace.h"



/* exported datatypes */


/**
 *  @brief A QPIPE stage is a queue of packets (work that must be
 *  completed) and a process_next_packet() function that worker
 *  threads can call to process the packets.
 */
class stage_t {


public:

    struct adaptor_t {

	typedef enum {
	    OUTPUT_RETURN_CONTINUE = 0,
	    OUTPUT_RETURN_STOP,
	    OUTPUT_RETURN_ERROR
	} output_t;
	
	virtual const char* get_container_name()=0;
        virtual packet_t* get_packet()=0;
        virtual output_t output(const tuple_t &tuple)=0;

	virtual void stop_accepting_packets()=0;	

        virtual bool check_for_cancellation()=0;
    };


public:

    stage_t() { }
    virtual ~stage_t() { }
    
    
    /**
     *  @brief Process this packet. Use output() to output tuples
     *  produced.
     *
     *  @return 0 on success. Non-zero value on unrecoverable
     *  error. If this function returns error, the stage will
     *  terminate all queries that it is currently involved in
     *  computing.
     */
    virtual int process_packet(adaptor_t* adaptor)=0;
};



#include "namespace.h"
#endif
