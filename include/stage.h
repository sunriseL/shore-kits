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

    // related with the pool of threads
    int num_pool_threads;
    thread_t* threadPool[THREAD_POOL_MAX];
    pthread_mutex_t worker_creator_mutex;


    /* simple resource allocation fix */
    pthread_mutex_t worker_thread_count_mutex;
    int worker_thread_count;


public:
    const char* stage_name() const {return name;}
    unsigned int thread_pids[THREAD_POOL_MAX];


    /* basic queue management */
    stage_queue_t* queue;  
    virtual void enqueue(packet_t*)=0;
    virtual int dequeue()=0;


    virtual void init_pool(pthread_t* stage_handles, int n_threads = DEFAULT_POOL_THREADS);
    virtual void stage_info(char* dest, bool bConstr);
    virtual void send_stage_constr();
    virtual void send_stage_destr();

    stage_t(const char* sname);
    virtual ~stage_t();


    int acquire_threads(int n);
    int release_thread(void);
    int release_threads(int n);
};

#include "namespace.h"
#endif
