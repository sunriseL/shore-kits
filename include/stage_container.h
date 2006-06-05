/* -*- mode:C++; c-basic-offset:4 -*- */

#ifndef _STAGE_CONTAINTER_H
#define _STAGE_CONTAINTER_H

#include "thread.h"
#include "packet.h"
#include "stage_factory.h"



using std::list;



// include me last!!!
#include "namespace.h"



/* exported datatypes */


class stage_container_t {

    struct stage_adaptor_t : public stage_t::adaptor_t {

        stage_container_t *_container;
        stage_t *_stage;
        packet_list_t *_packet_list;
        packet_t *_packet;
        volatile bool _cancelled;

        stage_adaptor_t(stage_container_t *container,
                        stage_t *stage,
                        packet_list_t *packet_list)
            : _container(container), _stage(stage), _packet_list(packet_list)
        {
            packet_list_t::iterator it = packet_list->begin();
            _packet = *it;
            while(++it != packet_list->end()) 
                it->terminate_inputs();
        }
        
        virtual packet_t *get_packet() {
            return _packet;
        }
        virtual output_t output(const tuple_t &tuple) {
            return _container->output(_packet_list, tuple);
        }
        virtual bool check_for_cancellation() {
            return _cancelled;
        }
        virtual void abort_query() {
            _container->abort_query(_packet_list);
        }
    };
    
protected:
  
    // synch vars
    pthread_mutex_t _container_lock;
    pthread_cond_t  _container_queue_nonempty;

    char*                 _container_name;
    list <packet_list_t*> _container_queue;
    list <stage_t*>       _stages;

    stage_factory_t* _stage_maker;

    void container_queue_enqueue(packet_t* packet);
    packet_list_t* container_queue_dequeue();

  
public:

    stage_container_t(const char* container_name, stage_factory_t* stage_maker);
    ~stage_container_t();
  
    const char* get_name(){ return _container_name; }

    void enqueue(packet_t* packet);

    void run();

};



#include "namespace.h"



#endif
