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
