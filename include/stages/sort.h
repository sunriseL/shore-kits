// -*- mode:C++; c-basic-offset:4 -*-

#ifndef __SORT_H
#define __SORT_H

#include "stage.h"
#include "stages/merge.h"
#include "dispatcher.h"
#include "trace/trace.h"
#include "qpipe_panic.h"
#include <list>
#include <map>
#include <deque>
#include <string>


using namespace qpipe;



/* exported constants */

#define SORT_STAGE_NAME  "SORT"
#define SORT_PACKET_TYPE "SORT"



/* exported functions */


/**
 *@brief Packet definition for the sort stage
 */
struct sort_packet_t : public packet_t {
    static const char *PACKET_TYPE;
    tuple_buffer_t *input_buffer;
    tuple_comparator_t *compare;

    sort_packet_t(char *packet_id,
                  tuple_buffer_t *out_buffer,
                  tuple_buffer_t *client_buffer,
                  tuple_buffer_t *in_buffer,
                  tuple_filter_t *filt,
                  tuple_comparator_t *cmp)
	: packet_t(packet_id, SORT_PACKET_TYPE, out_buffer, filt, client_buffer),
          input_buffer(in_buffer),
          compare(cmp)
    {
    }

    virtual void terminate_inputs();
};



/**
 * @brief Sort stage that partitions the input into sorted runs and
 * merges them into a single output run.
 */
class sort_stage_t : public stage_t {
private:
    static const size_t MERGE_FACTOR;
    
    // state provided by the packet
    adaptor_t *_adaptor;
    tuple_buffer_t *_input_buffer;
    tuple_comparator_t *_comparator;
    size_t _tuple_size;

    
    struct run_info_t;

    typedef std::deque<std::string> name_list_t;
    typedef std::map<int, name_list_t> run_map_t;
    typedef std::list<run_info_t> run_list_t;
    typedef merge_packet_t::buffer_list_t buffer_list_t;

    // used to communicate with the monitor thread
    pthread_t _monitor_thread;
    pthread_mutex_t _monitor_lock;
    pthread_cond_t _monitor_notify;
    volatile bool _merge_finished;
    volatile bool _sorting_finished;
    volatile bool _early_termination;

    // merge management
    run_map_t _finished_merges;
    run_list_t _current_merges;

public:
    static const char *DEFAULT_STAGE_NAME;

    ~sort_stage_t() {
        // make sure the monitor thread exits before we do...
        if(pthread_join(_monitor_thread, NULL)) {
            TRACE(TRACE_ALWAYS, "sort stage unable to join on monitor thread");
            QPIPE_PANIC();
        }
    }
    
protected:
    virtual int process_packet(adaptor_t *adaptor);

private:
    bool final_merge_ready();
    int create_sorted_run(int page_count);

    tuple_buffer_t *monitor_merge_packets();

    void check_finished_merges();
    void start_new_merges();
    void start_merge(run_map_t::iterator entry, int merge_factor);
};



#endif
