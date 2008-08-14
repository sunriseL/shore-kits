/* -*- mode:C++; c-basic-offset:4 -*- */

#ifndef __STAGE_CONTAINTER_H
#define __STAGE_CONTAINTER_H

#include "util.h"
#include "core/packet.h"
#include "core/stage.h"

using std::list;


ENTER_NAMESPACE(qpipe);


/* exported datatypes */

struct stage_factory_t;

class stage_container_t {
public:   
    static const unsigned int NEXT_TUPLE_UNINITIALIZED;
    static const unsigned int NEXT_TUPLE_INITIAL_VALUE;

    enum merge_t {
        MERGE_FAILED,
        MERGE_SUCCESS_HOLD_RESOURCES,
        MERGE_SUCCESS_RELEASE_RESOURCES
    };
	
protected:
    class stage_adaptor_t;
    typedef list <packet_list_t*> ContainerQueue;

    // used by the distributed stage queue
    struct DistributedQueueSlice {
	pthread_mutex_t _lock;
	pthread_cond_t _cond;
	ContainerQueue _queue;
	int _dequeue_op_count;
	int _dequeue_wait_count;
	DistributedQueueSlice()
	    : _lock(thread_mutex_create()), _cond(thread_cond_create()),
	      _dequeue_op_count(0), _dequeue_wait_count(0)
	{
	}
	
    private:
	// no-nos
	void operator=(DistributedQueueSlice const&);
	DistributedQueueSlice(DistributedQueueSlice const&);
    };

    vector<DistributedQueueSlice*> _queue_slices;
    
    // container synch vars
    debug_mutex_t _container_lock;
    pthread_cond_t  _container_queue_nonempty;
    
    c_str _container_name;
    //    ContainerQueue          _container_queue;

    list <stage_adaptor_t*> _container_current_stages;

    stage_factory_t* _stage_maker;

    // container queue manipulation
    void container_queue_enqueue_no_merge(packet_list_t* packets);
    void container_queue_enqueue_no_merge(packet_t* packet);
    packet_list_t* container_queue_dequeue(int which);
    void create_worker();   
    
public:    
    /* -ngm- Used for plain operations in the qpipe-plain branch. */
    static void* static_run_stage_wrapper(stage_t* stage,
                                          stage_adaptor_t* adaptor);

    stage_container_t(const c_str &container_name, stage_factory_t* stage_maker,
		      int active_count, int max_count=-1);

    ~stage_container_t();
  
    const c_str &get_name(){ return (_container_name); }

    void enqueue(packet_t* packet);

    void reserve(int n);
    void unreserve(int n);
    
    void run(int id);

private:

    void _reserve(int n);

    /* The pool that the worker threads will belong to. Thread pools
       are used to control the number of threads that the OS needs to
       schedule. When one thread in the pool waits on a condition
       variable, it let's another thread through. */
    thread_pool _pool;

    /* The maximum number of worker threads this stage should ever
       have. Note that even though this number is high, _pool
       guarantees that only a fixed number of them are being scheduled
       by the OS. */
    int _max_threads;

    /* Used to generate a container-unique thread ID when we create
       more threads. */
    int _next_thread;

    /* Used to track the reservation of worker threads by clients who
     * want to submit queries. A client is responsible for reserving
     * all required workers before submitting the query.
     *
     * This pool's capacity should be the number of worker threads
     * that exist for this stage. Every packet list in the container
     * queue must have a worker reserved for it.
     *
     * Workers can report themselves as idle or non-idle when they
     * enter and exit process_packet().
     */
    resource_pool_t _rp;

}; // EOF: stage_container_t



class stage_container_t::stage_adaptor_t : public stage_t::adaptor_t 
{
protected:
        
    // adaptor synch vars
    pthread_mutex_t _stage_adaptor_lock;


    stage_container_t* _container;

    packet_t*      _packet;
    packet_list_t* _packet_list;

    unsigned int _next_tuple;
    bool _still_accepting_packets;
    bool _contains_late_merger;

    // Group many output() tuples into a page before "sending"
    // entire page to packet list
    guard<page> out_page;
	
    // Checked independently of other variables. Don't need to
    // protect this with _stage_adaptor_mutex.
    volatile bool _cancelled;

	
public:

    stage_adaptor_t(stage_container_t* container,
		    packet_list_t* packet_list,
		    size_t tuple_size);
        

    ~stage_adaptor_t() {
	// we should have deleted the primary packet
	assert( _packet == NULL );
	// we should have either deleted or handed off ownership
	// of the packet list
	assert( _packet_list == NULL );
    }


    virtual const c_str &get_container_name() {
	return _container->get_name();
    }


    virtual packet_t* get_packet() {
	return _packet;
    }

        
    /**
     *  @brief Thin wrapper that just invokes output_page.
     *  Hopefully, this function will be inline and
     *  the compiler can optimize across the call to
     *  output_page, which is not virtual.
     */
    virtual void output(page* p) {
	output_page(p);
    }
	

    virtual void stop_accepting_packets() {
	critical_section_t cs(_stage_adaptor_lock);
	_still_accepting_packets = false;
    }
	

    virtual bool check_for_cancellation() {
	return _cancelled;
    }


    stage_container_t::merge_t try_merge(packet_t* packet);
    void run_stage(stage_t* stage);
    
protected:

    void finish_packet(packet_t* packet);
    void cleanup();
    void abort_queries();

private:

    void output_page(page* p);

}; // EOF: stage_container_t::stage_adaptor_t


struct stage_factory_t 
{
  virtual stage_t* create_stage()=0;
  virtual ~stage_factory_t() { }

}; // EOF: stage_factory_t



/**
 *  @brief If a stage provides DEFAULT_STAGE_NAME and does not need
 *  any additional constructor parameters, we actually write a lot of
 *  uncessary code creating its factory. We create this stage_factory
 *  (NOTE NO "_t"!) to provide a quick and dirty template
 *  workaround. For example, we could type:
 *
 *  dispatcher_t::register_stage(new stage_factory<sort_stage_t>);
 *
 *  The template specifies which stage to construct.
 */
template<class STAGE>
struct stage_factory : public stage_factory_t 
{
  virtual stage_t* create_stage() {
    return new STAGE();
  }

  virtual ~stage_factory() { }

}; // EOF: stage_factory


EXIT_NAMESPACE(qpipe);


#endif
