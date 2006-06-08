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

using std::string;



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

    
    /**
     * @brief stores information about an in-progress merge run
     */
    struct run_info_t {
        int _merge_level;
        string _file_name;
        // this output buffer never sends data -- just watch for eof
        tuple_buffer_t *_buffer;

        run_info_t(int level, const string &name, tuple_buffer_t *buf)
            : _merge_level(level), _file_name(name), _buffer(buf)
        {
        }
    };

    typedef std::deque<std::string> name_list_t;
    typedef std::map<int, name_list_t> run_map_t;
    typedef std::map<tuple_buffer_t*, name_list_t> file_map_t;
    typedef std::list<run_info_t> run_list_t;
    typedef merge_packet_t::buffer_list_t buffer_list_t;
    typedef std::vector<key_tuple_pair_t> key_vector_t;

    // used to communicate with the monitor thread
    pthread_t _monitor_thread;
    notify_t _monitor;
    volatile bool _sorting_finished;

    // merge management
    run_map_t _finished_merges;
    run_list_t _current_merges;
    file_map_t _merge_inputs;

public:

    static const char* DEFAULT_STAGE_NAME;

    ~sort_stage_t();

protected:

    virtual int process_packet();
    
private:

    void remove_input_files(tuple_buffer_t *buf);
    bool final_merge_ready();
    int create_sorted_run(int page_count);

    tuple_buffer_t *monitor_merge_packets();

    void check_finished_merges();
    void start_new_merges();
    void start_merge(run_map_t::iterator entry, int merge_factor);
};



#endif
