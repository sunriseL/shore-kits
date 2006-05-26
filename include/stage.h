/* -*- C++ -*- */
#ifndef _STAGE_H
#define _STAGE_H

#include "thread.h"
#include "tuple.h"
#include "packet.h"
#include <list>

// include me last!!!
#include "namespace.h"
using std::list;

#define THREAD_POOL_MAX 10
#define DEFAULT_POOL_THREADS 8

/** Automatically closes an input buffer when it goes out of scope */
struct buffer_closer_t {
    tuple_buffer_t *buffer;
    buffer_closer_t(tuple_buffer_t *buf)
        : buffer(buf)
    {
    }

    ~buffer_closer_t() {
        buffer->close_buffer();
    }
    tuple_buffer_t *operator->() {
        return buffer;
    }
};

/*
 * Class of a basic queue for the stages. In the current implementation it
 * uses the basic_list_t implementation of a list. Uses mutexes and condition
 * variables for synchronization.
 */

class stage_queue_t {

private:
    typedef list<packet_t*> packet_list_t;
    
    pthread_mutex_t queue_mutex;
    pthread_cond_t  queue_packet_available;

    int num_packets;
    packet_list_t packet_list;

    const char* stagename;

public:

    void insert(packet_t* packet);
    packet_t* remove();
    packet_t* remove(packet_t* packet);
    packet_t* find_and_link_mergable(packet_t* packet);
    int length();

    stage_queue_t(const char* sname);
    ~stage_queue_t();
};


/**
 * class stage
 *
 * Interface for the various stages. Each stage has a descriptive name 
 * and consists of a pool of threads, and a basic queue. 
 */

class stage_t {
public:
    typedef enum {
        STAGE_IPROBE = 1,
        STAGE_INSERT,
        STAGE_UPDATE,
        STAGE_DELETE,
        STAGE_SCAN,
        STAGE_AGGREGATE,
        STAGE_SORT,
        STAGE_JOIN,
        STAGE_OTHER,
    } type_t;

protected:
    // general information about the stage
    const char* name;
    type_t stage_type;

public:
    const char* stage_name() const {return name;}


    /* basic queue management */
    stage_queue_t* queue;  
    virtual void enqueue(packet_t*)=0;
    int process_next_packet();

    stage_t(const char* sname);
    virtual ~stage_t();

protected:
    virtual int process_packet(packet_t *packet)=0;
    
    /** @brief outputs a tuple to any waiting buffers
     * @return non-zero if output failed (early termination)
     */
    int output(packet_t *packet, const tuple_t &tuple);

    /** @brief cleans up after completing work on a packet
     */
    void done(packet_t *packet);
};

#include "namespace.h"
#endif
