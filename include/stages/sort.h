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

using namespace std;


/* exported functions */


/**
 *@brief Packet definition for the sort stage
 */
struct sort_packet_t : public packet_t {

public:

    static const char *PACKET_TYPE;

    tuple_buffer_t *input_buffer;
    tuple_comparator_t *compare;

    sort_packet_t(char *packet_id,
                  tuple_buffer_t *out_buffer,
                  tuple_buffer_t *client_buffer,
                  tuple_buffer_t *in_buffer,
                  tuple_filter_t *filt,
                  tuple_comparator_t *cmp)
	: packet_t(packet_id, PACKET_TYPE, out_buffer, filt, client_buffer),
          input_buffer(in_buffer),
          compare(cmp)
    {
    }

    virtual void terminate_inputs() {
	// close the input buffer
	input_buffer->close();
    }

};



/**
 * @brief Sort stage that partitions the input into sorted runs and
 * merges them into a single output run.
 */
class sort_stage_t : public stage_t {

private:

    static const size_t MERGE_FACTOR;
    
    // state provided by the packet
    tuple_buffer_t *_input;
    tuple_comparator_t *_comparator;
    size_t _tuple_size;
    

    typedef list<string> run_list_t;


    // all information we need for an active merge
    struct merge_t {
	string _output; // name of output file
	run_list_t _inputs;
        tuple_buffer_t* _signal_buffer;
	
	merge_t () { }
        merge_t(const string &output, const run_list_t &inputs, tuple_buffer_t * signal_buffer)
            : _output(output),
	      _inputs(inputs),
	      _signal_buffer(signal_buffer)
        {
        }
    };

    
    typedef map<int, run_list_t> run_map_t;
    typedef list<merge_t> merge_list_t;
    typedef map<int, merge_list_t> merge_map_t;
    typedef merge_packet_t::buffer_list_t buffer_list_t;
    typedef vector<key_tuple_pair_t> key_vector_t;


    // used to communicate with the monitor thread
    pthread_t _monitor_thread;
    notify_t _monitor;
    volatile bool _sorting_finished;

    // merge management
    run_map_t _run_map;
    merge_map_t _merge_map;

public:

    static const char* DEFAULT_STAGE_NAME;

    ~sort_stage_t();

protected:

    virtual int process_packet();
    
private:

    bool final_merge_ready();
    int create_sorted_run(int page_count);

    tuple_buffer_t *monitor_merge_packets();

    void check_finished_merges();
    void start_new_merges();
    void start_merge(int new_level, run_list_t &runs, int merge_factor);
    void remove_input_files(run_list_t &files);

    // debug
    int print_runs();
    int print_merges();
};



#endif
