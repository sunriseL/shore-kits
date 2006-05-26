#include "stage.h"
#include "packet.h"

#include "namespace.h"

void stage_queue_t::insert(packet_t* packet)
{

  // Easy. Do a synchronized insert into the queue (append to the
  // list), update the packet count, and wake at most one thread
  // waiting on the queue.

  if (packet == NULL)
    {
      TRACE(TRACE_ALWAYS, "NULL packet!\n");
      return;
    }



  // acuire mutex
  pthread_mutex_lock_wrapper(&queue_mutex);


  if ( basic_list_append( packet_list, packet ) ) {
    fprintf(stderr, "Error: basic_list_append failed\n");
  }

  num_packets++;
  if ( pthread_cond_signal_wrapper( &queue_packet_available ) ) {
    fprintf(stderr, "Error: pthread_cond_signal failed\n");
  }

  // release mutex
  pthread_mutex_unlock_wrapper(&queue_mutex);
}




/**
 * remove: Remove a packet from the queue
 */

packet_t* stage_queue_t::remove()
{
  // wait for a packet to appear. Then remove it and return.

  // wait for a packet
  pthread_mutex_lock_wrapper(&queue_mutex);

  
  while ( basic_list_is_empty( packet_list ) ) {
    if ( pthread_cond_wait_wrapper( &queue_packet_available, &queue_mutex ) ) {
      fprintf(stderr, "Error: pthread_cond_wait failed\n");
    }
  }


  // remove packet
  packet_t *p;
  if ( basic_list_remove_head( packet_list, (void**)&p ) ) {
    fprintf(stderr, "basic_list_remove_head failed on a non-empty list\n");
  }
  num_packets--;


  pthread_mutex_unlock_wrapper(&queue_mutex);
  return p;
}


// INTEG: Copied Nikos
packet_t* stage_queue_t::remove(packet_t* packet)
{
  if ( packet == NULL )
    {
      fprintf(stderr, "Warning: stage_queue_t::remove(packet) called with NULL packet\n");
      return NULL;
    }


  // Step 1: wait for a packet
  pthread_mutex_lock_wrapper(&queue_mutex);

  while ( basic_list_is_empty( packet_list ) )
    {
      if ( pthread_cond_wait_wrapper( &queue_packet_available, &queue_mutex ) )
	{
	  fprintf(stderr, "Error: pthread_cond_wait failed\n");
	}
    }



  // Step 2: Remove the specified packet from the list, if it
  // exists. Equality is pointer equality.
  if ( basic_list_remove( packet_list, pointer_equality_comparator, packet, NULL ) )
    {
      // packet not found
      fprintf(stderr, "Warning: stage_queue_t::remove: Packet not found in list\n");
      pthread_mutex_unlock_wrapper(&queue_mutex);
      return NULL;
    }
  num_packets--;



  pthread_mutex_unlock_wrapper(&queue_mutex);
  return packet;

}


/*
 * try to find a packet to link with. Return the linked packet on
 * success. NULL if one is not found.
 */

packet_t* stage_queue_t::find_and_link_mergable(packet_t* packet)
{
  // Does not wait for a packet

  pthread_mutex_lock_wrapper(&queue_mutex);


  // empty list case
  if ( basic_list_is_empty( packet_list ) )
    {
      pthread_mutex_unlock_wrapper(&queue_mutex);
      return NULL;
    }


  merger_state_t merge_state = { packet, NULL };
  basic_list_process( packet_list, merge_mapper, &merge_state );



  // release mutex
  pthread_mutex_unlock_wrapper(&queue_mutex);

  return merge_state.mergable_packet;
}


/**
 * Constructor initializes counters, mutexes, and semaphor
 */

stage_queue_t::stage_queue_t(const char* sname) {

  num_packets = 0;
  packet_list = basic_list_create();
  stagename = sname;

  if ( packet_list == NULL )
    {
      fprintf(stderr, "Error: basic_list_create failed\n");
    }

  if (pthread_mutex_init_wrapper(&queue_mutex, NULL))
    {
      fprintf(stderr, "[stage_queue_t constructor] Error in pthread_mutex_init()\n");
    }

  if (pthread_cond_init_wrapper(&queue_packet_available,NULL))
    {
      fprintf(stderr, "[stage_queue_t constructor] Error in pthread_cond_init()\n");
    }
}



/**
 * Destructor: destroys the mutex and the semaphor
 */ 

stage_queue_t::~stage_queue_t(){

  basic_list_destroy(packet_list);
  pthread_mutex_destroy_wrapper(&queue_mutex);
  pthread_cond_destroy_wrapper (&queue_packet_available);
}





#include "namespace.h"
