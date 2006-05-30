
#include "stage.h"
#include "packet.h"
#include "trace/trace.h"
#include "qpipe_panic.h"


// include me last!!!
#include "namespace.h"




/**
 *  @brief Create a new queue with no packets.
 */
stage_queue_t::stage_queue_t(stage_t* stage)
{
  parent_stage = stage;
  
  pthread_mutex_init_wrapper( &queue_mutex, NULL );
  pthread_cond_init_wrapper ( &queue_packet_available, NULL );

  //  new (&packet_list) packet_list_t();
}




/**
 *  @brief Destroy this queue of packets. This function currently
 *  fails if the queue contains any packets.
 */
stage_queue_t::~stage_queue_t(void)
{
  // Do not invoke parent stage destructor. We are probably being
  // destroyed because that destructor was already called.

  pthread_mutex_destroy_wrapper(&queue_mutex);
  pthread_cond_destroy_wrapper (&queue_packet_available);

  TRACE(TRACE_ALWAYS, "stage_queue_t destructor not fully implemented... need to deal with packets\n");
}




/**
 *  @brief Append a new packet to the queue.
 */
void stage_queue_t::enqueue(packet_t* packet)
{

  // error checking
  if (packet == NULL)
  {
    TRACE(TRACE_ALWAYS, "Called with NULL packet\n");
    QPIPE_PANIC();
  }
  
  // * * * BEGIN CRITICAL SECTION * * *
  pthread_mutex_lock_wrapper(&queue_mutex);

  packet_list.push_back( packet );
  
  // signal waiting thread
  pthread_cond_signal_wrapper( &queue_packet_available );
  
  pthread_mutex_unlock_wrapper(&queue_mutex);
  // * * * END CRITICAL SECTION * * *
}




/**
 *  @brief Remove the first packet in the queue.
 */
packet_t* stage_queue_t::dequeue(void)
{

  // * * * BEGIN CRITICAL SECTION * * *
  pthread_mutex_lock_wrapper(&queue_mutex);

  while ( packet_list.empty() )
  {
    pthread_cond_wait_wrapper( &queue_packet_available, &queue_mutex );
  }

  // remove packet
  packet_t* p = packet_list.front();
  packet_list.pop_front();

  pthread_mutex_unlock_wrapper(&queue_mutex);
  // * * * END CRITICAL SECTION * * *

  return p;
}




/**
 *  @brief Remove the specified packet from the queue, if it exists.
 */
void stage_queue_t::remove_copies(packet_t* packet)
{
  
  if ( packet == NULL )
  {
    TRACE(TRACE_ALWAYS, "Called with NULL packet\n");
    QPIPE_PANIC();
  }

  // * * * BEGIN CRITICAL SECTION * * *
  pthread_mutex_lock_wrapper(&queue_mutex);

  packet_list.remove(packet);      

  pthread_mutex_unlock_wrapper(&queue_mutex);
  // * * * END CRITICAL SECTION * * *
}




/**
 *  @brief Search the queue for packets that can be merged with the
 *  specified packet. If a mergeable packet P is found, link the
 *  specifed packet to P and return P. Return NULL if no merge took
 *  place.
 */
packet_t* stage_queue_t::find_and_merge(packet_t* packet)
{

  if ( packet == NULL )
  {
    TRACE(TRACE_ALWAYS, "Called with NULL packet\n");
    QPIPE_PANIC();
  }


  // nothing merged yet
  packet_t* m = NULL;


  // * * * BEGIN CRITICAL SECTION * * *
  pthread_mutex_lock_wrapper(&queue_mutex);

  packet_list_t::iterator it;
  for (it = packet_list.begin(); it != packet_list.end(); ++it)
  {
    packet_t* p = *it;
    if ( p->is_mergeable(packet) )
    {
      p->merge(packet);
      m = p;
      break;
    }
  }

  pthread_mutex_unlock_wrapper(&queue_mutex);
  // * * * END CRITICAL SECTION * * *


  return m;
}




#include "namespace.h"
