// -*- mode:C++; c-basic-offset:4 -*-

#include "thread.h"
#include "tester_thread.h"
#include "stages/merge.h"
#include "trace/trace.h"
#include "qpipe_panic.h"
#include <utility>
#include <vector>
#include <algorithm>
#include "stage_container.h"

using namespace qpipe;

using std::vector;
using std::pair;

/**
 *  @brief Simulates a worker thread on the specified stage.
 *
 *  @param arg A stage_t* to work on.
 */
void* drive_stage(void* arg)
{
  stage_container_t* sc = (stage_container_t*)arg;
  sc->run();

  return NULL;
}


typedef vector<int> input_list_t;
typedef pair<tuple_buffer_t*, input_list_t> write_info_t;


void* write_tuples(void* arg)
{
    write_info_t *info = (write_info_t *)arg;
    tuple_buffer_t *buffer = info->first;
    input_list_t &inputs = info->second;

    // wait for the parent to wake me up...
    buffer->wait_init();
    
    int value;
    tuple_t input((char *)&value, sizeof(int));
    for(input_list_t::iterator it=inputs.begin(); it != inputs.end(); ++it) {
        value = *it;
        if(buffer->put_tuple(input))
            {
                TRACE(TRACE_ALWAYS, "buffer->put_tuple() returned non-zero!\n");
                TRACE(TRACE_ALWAYS, "Terminating loop...\n");
                break;
            }
        //        else
        //            TRACE(TRACE_ALWAYS, "Inserted tuple %d\n", value);
    }

    TRACE(TRACE_ALWAYS, "Done inserting tuples\n");
    buffer->send_eof();
    return NULL;
}


struct int_tuple_comparator_t : public tuple_comparator_t {
    virtual int compare(const key_tuple_pair_t &a, const key_tuple_pair_t &b) {
        return a.key - b.key;
    }
    virtual int make_key(const tuple_t &tuple) {
        return *(int*)tuple.data;
    }
};

int main() {

    thread_init();

    stage_container_t* sc = new stage_container_t("MERGE_CONTAINER",
                                                  new stage_factory<merge_stage_t>);
    tester_thread_t* merge_thread = new tester_thread_t(drive_stage, sc, "MERGE THREAD");
    if ( thread_create( NULL, merge_thread ) ) {
        TRACE(TRACE_ALWAYS, "thread_create failed\n");
        QPIPE_PANIC();
    }

    static const int merge_factor = 5;
    static const int count = 200000;
    write_info_t merge_info[merge_factor];
    
    // generate a list of count unique tuples with merge_factor
    // duplicates of each
    input_list_t main_inputs;
    for(int i=0; i < count; i++)
        for(int j=0; j < merge_factor; j++)
            main_inputs.push_back(i);

    // shuffle the list and partition it
    std::random_shuffle(main_inputs.begin(), main_inputs.end());
    int i=0;
    input_list_t::iterator it=main_inputs.begin();
    while(it != main_inputs.end()) {
        input_list_t &input_list = merge_info[i].second;
        std::sort(it, it+count);
        input_list.insert(input_list.end(), it, it+count);
        i++;
        it += count;
    }
    
    // send the partitions to merge input generation threads
    merge_packet_t::buffer_list_t input_buffers;
    for(int i=0; i < merge_factor; i++) {
        tuple_buffer_t  *input_buffer = new tuple_buffer_t(sizeof(int));
        input_buffers.push_back(input_buffer);
        merge_info[i].first = input_buffer;
        tester_thread_t* writer_thread = new tester_thread_t(write_tuples, &merge_info[i], "WRITER THREAD");
        if ( thread_create( NULL, writer_thread ) ) {
            TRACE(TRACE_ALWAYS, "thread_create failed\n");
            QPIPE_PANIC();
        }
    }

    // fire up the merge stage now
    tuple_buffer_t  output_buffer(sizeof(int));
    tuple_filter_t* filter = new tuple_filter_t(input_buffers[0]->tuple_size);
    int_tuple_comparator_t comparator;

    merge_packet_t* packet = new merge_packet_t("test merge",
                                                &output_buffer,
                                                &output_buffer,
                                                input_buffers,
                                                filter,
                                                merge_factor,
                                                &comparator);

    sc->enqueue(packet);
    
    tuple_t output;
    output_buffer.init_buffer();
    while(output_buffer.get_tuple(output)) {
    //        TRACE(TRACE_ALWAYS, "Value: %d\n", *(int*)output.data);
    }
    TRACE(TRACE_ALWAYS, "Output complete\n");

    return 0;
}
