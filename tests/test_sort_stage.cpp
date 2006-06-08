// -*- mode:C++; c-basic-offset:4 -*-

#include "thread.h"
#include "tester_thread.h"
#include "stages/sort.h"
#include "stages/fscan.h"
#include "stages/fdump.h"
#include "stages/merge.h"
#include "trace/trace.h"
#include "qpipe_panic.h"
#include "stage_container.h"

#include <vector>
#include <algorithm>

using std::vector;

using namespace qpipe;


int values = 1800;


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


void* write_tuples(void* arg)
{
    tuple_buffer_t *buffer = (tuple_buffer_t *)arg;

    // produce a set of inputs, with duplicated values
    static const int copies = 5;
    vector<int> inputs;
    inputs.reserve(values*copies);
    for(int i=0; i < values; i++)
        for(int j=0; j < copies; j++)
            inputs.push_back(i);

    // shuffle the input
    std::random_shuffle(inputs.begin(), inputs.end());

    // wait for the parent to wake me up...
    buffer->wait_init();
    
    int value;
    tuple_t input((char *)&value, sizeof(int));
    for(unsigned  i=0; i < inputs.size(); i++) {
        value = inputs[i];
        if(buffer->put_tuple(input))
            {
                TRACE(TRACE_ALWAYS, "buffer->put_tuple() returned non-zero!\n");
                TRACE(TRACE_ALWAYS, "Terminating loop...\n");
                break;
            }
        //        else
        //            TRACE(TRACE_ALWAYS, "Inserted tuple %d\n", value);
    }
    TRACE(TRACE_ALWAYS, "Finished inserting tuples\n", value);
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



int main(int argc, char* argv[]) {

    thread_init();
    

    // usage: test_sort_stage <values> [off]
    if ( argc >= 2 ) {
	values = atoi(argv[1]);
	if (values == 0) {
	    TRACE(TRACE_ALWAYS, "%s is not a number\n", argv[1]);
	    exit(-1);
	}
    }
    bool do_echo = true;
    if ( (argc >= 3) && !strcmp(argv[2], "off") )
	do_echo = false;



    stage_container_t* sc;

    sc = new stage_container_t("FDUMP_CONTAINER", new stage_factory<fdump_stage_t>);
    dispatcher_t::register_stage_container(fdump_packet_t::PACKET_TYPE, sc);
    tester_thread_t* fdump_thread = new tester_thread_t(drive_stage, sc, "FDUMP THREAD");
    if ( thread_create( NULL, fdump_thread ) ) {
        TRACE(TRACE_ALWAYS, "thread_create failed\n");
        QPIPE_PANIC();
    }
    
    sc = new stage_container_t("FSCAN_CONTAINER", new stage_factory<fscan_stage_t>);
    dispatcher_t::register_stage_container(fscan_packet_t::PACKET_TYPE, sc);
    tester_thread_t* fscan_thread = new tester_thread_t(drive_stage, sc, "FSCAN THREAD");
    if ( thread_create( NULL, fscan_thread ) ) {
        TRACE(TRACE_ALWAYS, "thread_create failed\n");
        QPIPE_PANIC();
    }
    
    sc = new stage_container_t("MERGE_CONTAINER", new stage_factory<merge_stage_t>);
    dispatcher_t::register_stage_container(merge_packet_t::PACKET_TYPE, sc);
    tester_thread_t* merge_thread = new tester_thread_t(drive_stage, sc, "MERGE THREAD");
    if ( thread_create( NULL, merge_thread ) ) {
        TRACE(TRACE_ALWAYS, "thread_create failed\n");
        QPIPE_PANIC();
    }

    sc = new stage_container_t("SORT_CONTAINER", new stage_factory<sort_stage_t>);
    dispatcher_t::register_stage_container(sort_packet_t::PACKET_TYPE, sc);
    tester_thread_t* sort_thread = new tester_thread_t(drive_stage, sc, "SORT THREAD");
    if ( thread_create( NULL, sort_thread ) ) {
        TRACE(TRACE_ALWAYS, "thread_create failed\n");
        QPIPE_PANIC();
    }


    // for now just sort straight tuples
    tuple_buffer_t  input_buffer(sizeof(int));
    tester_thread_t* writer_thread = new tester_thread_t(write_tuples, &input_buffer, "WRITER THREAD");
    if ( thread_create( NULL, writer_thread ) ) {
        TRACE(TRACE_ALWAYS, "thread_create failed\n");
        QPIPE_PANIC();
    }

   
    // the output is always a single int
    tuple_buffer_t  output_buffer(sizeof(int));
    tuple_filter_t* filter = new tuple_filter_t();
    int_tuple_comparator_t *compare = new int_tuple_comparator_t;

    sort_packet_t* packet = new sort_packet_t("test sort",
                                              &output_buffer,
                                              &output_buffer,
                                              &input_buffer,
                                              filter,
                                              compare);

    dispatcher_t::dispatch_packet(packet);
    
    tuple_t output;
    output_buffer.init_buffer();
    while(output_buffer.get_tuple(output)) {
	if (do_echo)
	    TRACE(TRACE_ALWAYS, "Count: %d\n", *(int*)output.data);
    }

    TRACE(TRACE_ALWAYS, "TEST_DONE\n");
    return 0;
}
