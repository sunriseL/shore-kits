// -*- mode:C++; c-basic-offset:4 -*-

#ifndef __SORT_H
#define __SORT_H

#include "stage.h"
#include "stages/merge.h"
#include "dispatcher.h"
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
                  tuple_buffer_t *in_buffer,
                  tuple_filter_t *filt,
                  tuple_comparator_t *cmp)
	: packet_t(packet_id, SORT_PACKET_TYPE, out_buffer, filt),
          input_buffer(in_buffer),
          compare(cmp)
    {
    }

    virtual void terminate();
};



/**
 * @brief Sort stage that partitions the input into sorted runs and
 * merges them into a single output run.
 */
class sort_stage_t : public stage_t {
private:
    // state provided by the packet
    tuple_buffer_t *_input_buffer;
    tuple_comparator_t *_comparator;
    size_t _tuple_size;

    
    struct run_info_t;

    typedef std::deque<std::string> name_list_t;
    typedef std::map<int, name_list_t> run_map_t;
    typedef std::list<run_info_t> run_list_t;
    typedef merge_packet_t::buffer_list_t buffer_list_t;
    
    
public:
    static const char *DEFAULT_STAGE_NAME;
    sort_stage_t(packet_list_t *packets,
                 stage_container_t *container,
                 const char *name)
        : stage_t(packets, container, name, true)
    {
    }

protected:
    virtual int process();
    virtual void terminate_stage();

private:
    int create_sorted_run(int page_count);
};



#endif
