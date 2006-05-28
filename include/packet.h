// -*- C++ -*-
#ifndef __PACKET_H
#define __PACKET_H

#include "db_cxx.h"
#include "tuple.h"
#include "functors.h"
#include "qpipe_panic.h"


#include "namespace.h"


/**
 *  @brief A packet in QPIPE is a unit of work that can be processed
 *  by a stage's worker thread.
 */
class packet_t
{

public:

  DbTxn* xact_id;
  char*  packet_id;
  tuple_buffer_t* output_buffer;
  tuple_filter_t* filter;
  
  // mutex to protect the chain of merged packets
  pthread_mutex_t merge_mutex;
  packet_t*       next_merged_packet;
  int             merge_set_size;
  
public:

  packet_t(DbTxn* dbt,
	   char* pack_id,
	   tuple_buffer_t* outbuf,
	   tuple_filter_t *filt);

  virtual ~packet_t(void);

  /**
   *  @brief Check whether this packet can be merged with the
   *  specified one. By default, packets are not mergeable.
   *
   *  @return false
   */  
  virtual bool is_mergable(packet_t* packet) { packet=packet; return false; }

  virtual void merge(packet_t* packet);

  /**
   *  @brief Packet ID accessor.
   */
  char* get_packet_id(void) { return packet_id; }

};

#include "namespace.h"
#endif
