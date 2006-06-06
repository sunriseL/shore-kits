/* -*- mode:C++; c-basic-offset:4 -*- */

#ifndef _STAGE_CONTAINTER_H
#define _STAGE_CONTAINTER_H

#include "thread.h"
#include "packet.h"
#include "stage_factory.h"
#include "stage.h"
#include "utils.h"



using std::list;



// include me last!!!
#include "namespace.h"



/* exported datatypes */


class stage_container_t {

protected:

    class stage_adaptor_t : public stage_t::adaptor_t {
	
    protected:

	// synch vars
	pthread_mutex_t _stage_adaptor_lock;

	stage_container_t* _container;
        packet_list_t* _packet_list;
	int _next_tuple;
        bool _still_accepting_packets;
	
	// Checked independently of other variables. Don't need to
	// protect this with _stage_adaptor_mutex.
	volatile bool _cancelled;

        packet_t* _packet;
	
    public:

        stage_adaptor_t(stage_container_t* container,
                        packet_list_t* packet_list)
            : _container(container),
	      _packet_list(packet_list),
	      _next_tuple(1),
	      _still_accepting_packets(true),
	      _cancelled(false)
        {

	    pthread_mutex_init_wrapper(&_stage_adaptor_lock, NULL);

            packet_list_t::iterator it;

	    // terminate all inputs except one
	    it = packet_list->begin();
            _packet = *it;
            while(++it != packet_list->end()) 
                (*it)->terminate_inputs();

	    // record next_tuple
	    for (it = packet_list->begin(); it != packet_list->end(); ++it) {
		(*it)->_stage_next_tuple_on_merge = 1;
	    }
        }
        
	virtual const char* get_container_name() {
	    return _container->get_name();
	}

        virtual packet_t* get_packet() {
            return _packet;
        }

        virtual output_t output(const tuple_t &tuple);
	
	virtual void stop_accepting_packets() {
	    critical_section_t cs(&_stage_adaptor_lock);
	    _still_accepting_packets = false;
	}
	
        virtual bool check_for_cancellation() {
            return _cancelled;
        }

	bool try_merge(packet_t* packet);

	void run_stage(stage_t* stage);

    protected:

	void terminate_packet(packet_t* packet, int stage_done);

        void abort_queries();
    };
    
  
    // synch vars
    pthread_mutex_t _container_lock;
    pthread_cond_t  _container_queue_nonempty;

    char*                   _container_name;
    list <packet_list_t*>   _container_queue;
    list <stage_adaptor_t*> _container_current_stages;

    stage_factory_t* _stage_maker;


    // container queue manipulation
    void container_queue_enqueue_no_merge(packet_list_t* packets);
    void container_queue_enqueue(packet_t* packet);
    packet_list_t* container_queue_dequeue();

    stage_t::adaptor_t::output_t output(packet_list_t* packets, const tuple_t &tuple);
    void abort_query(packet_list_t* packets);

public:

    stage_container_t(const char* container_name, stage_factory_t* stage_maker);
    ~stage_container_t();
  
    const char* get_name(){ return _container_name; }

    void enqueue(packet_t* packet);

    void run();
};



#include "namespace.h"



#endif
