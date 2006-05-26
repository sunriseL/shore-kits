// -*- C++ -*-
#ifndef __PACKET_H
#define __PACKET_H

#include "db_cxx.h"
#include "tuple.h"
#include "thread.h"

#include "namespace.h"

/**
 * class packet_t
 *
 * Class of a stage packet
 */

class packet_t {
public:
    DbTxn* xact_id;
    char* packet_string_id;
    int unique_id;
    tuple_buffer_t* output_buffer;
  
    // mutex to protect the list of merged packets
    pthread_mutex_t mutex;
    packet_t* next_packet;
    int num_merged_packets;
  
    //    dispatcher_cpu_t bind_cpu;


public:

    packet_t(DbTxn* tid, char* packet_id, tuple_buffer_t* out_buffer);

    virtual ~packet_t();
  
    virtual bool mergable(packet_t* packet) { return false; }

    virtual void link_packet(packet_t* packet);

    int get_unique_id() { return (unique_id); }
};

#include "namespace.h"
#endif
