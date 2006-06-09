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

    typedef enum {
	RESULT_CONTINUE = 0,
	RESULT_STOP,
	RESULT_ERROR
    } result_t;


    struct adaptor_t {

	virtual const char* get_container_name()=0;
        virtual packet_t* get_packet()=0;
        virtual result_t output(const tuple_t &tuple)=0;

	virtual void stop_accepting_packets()=0;	

        virtual bool check_for_cancellation()=0;
    };


protected:

    adaptor_t* _adaptor;

    virtual result_t process_packet()=0;

public:

    stage_t()
	: _adaptor(NULL)
    {
    }

    virtual ~stage_t() { }
    

    void init(adaptor_t* adaptor) {
	_adaptor = adaptor;
    }    


    /**
     *  @brief Process this packet. The stage container must invoke
     *  init_stage() with an adaptor that we can use.
     *
     *  @return 0 on success. Non-zero value on unrecoverable
     *  error. If this function returns error, the stage will
     *  terminate all queries that it is currently involved in
     *  computing.
     */
    result_t process() {
	assert(_adaptor != NULL);
	return process_packet();
    }
};



#include "namespace.h"
#endif
