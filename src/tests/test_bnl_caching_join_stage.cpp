/* -*- mode:C++; c-basic-offset:4 -*- */

#include "thread.h"
#include "stage_container.h"
#include "stages/bnl_caching_join.h"
#include "stages/fdump.h"
#include "trace/trace.h"
#include "qpipe_panic.h"

#include "dispatcher.h"
#include "tester_thread.h"



using namespace qpipe;



struct write_info_s {
    tuple_buffer_t* buffer;
    int num_tuples;
};


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
    struct write_info_s* info = (struct write_info_s*)arg;
    tuple_buffer_t* int_buffer = info->buffer;

    int_buffer->wait_init();
    
    for (int i = 0; i < info->num_tuples; i++) {
	tuple_t in_tuple((char*)&i, sizeof(int));
	if( int_buffer->put_tuple(in_tuple) ) {
	    TRACE(TRACE_ALWAYS, "tuple_page->append_init() returned non-zero!\n");
	    TRACE(TRACE_ALWAYS, "Terminating loop...\n");
	    break;
	}
    }
    
    int_buffer->send_eof();

    return NULL;
}



int main(int argc, char* argv[]) {

    thread_init();
    
    
    // parse output filename
    if ( argc < 3 ) {
	TRACE(TRACE_ALWAYS, "Usage: %s <count-left> <count-right>\n", argv[0]);
	exit(-1);
    }
    int count_left = atoi(argv[1]);
    if ( count_left == 0 ) {
	TRACE(TRACE_ALWAYS, "Invalid tuple count %s\n", argv[1]);
	exit(-1);
    }
    int count_right = atoi(argv[2]);
    if ( count_right == 0 ) {
	TRACE(TRACE_ALWAYS, "Invalid tuple count %s\n", argv[2]);
	exit(-1);
    }


    // create BNL_JOIN worker thread
    stage_container_t* join_sc =
	new stage_container_t("BNL_CACHING_JOIN_CONTAINER", new stage_factory<bnl_caching_join_stage_t>);
    dispatcher_t::register_stage_container(bnl_caching_join_packet_t::PACKET_TYPE, join_sc);
    tester_thread_t* join_thread = 
	new tester_thread_t(drive_stage, join_sc, "BNL_CACHING_JOIN_THREAD");
    if ( thread_create( NULL, join_thread ) ) {
	TRACE(TRACE_ALWAYS, "thread_create() failed\n");
	QPIPE_PANIC();
    }


    // create FDUMP thread
    stage_container_t* fdump_sc =
	new stage_container_t("FDUMP_CONTAINER", new stage_factory<fdump_stage_t>);
    dispatcher_t::register_stage_container(fdump_packet_t::PACKET_TYPE, fdump_sc);
    tester_thread_t* fdump_thread = 
	new tester_thread_t(drive_stage, fdump_sc, "FDUMP_THREAD");
    if ( thread_create( NULL, fdump_thread ) ) {
	TRACE(TRACE_ALWAYS, "thread_create() failed\n");
	QPIPE_PANIC();
    }
    

    // create WRITER_LEFT
    tuple_buffer_t writer_left_buffer(sizeof(int));
    struct write_info_s writer_left_info = { &writer_left_buffer, count_left };
    tester_thread_t* writer_left_thread =
	new tester_thread_t(write_tuples, &writer_left_info, "LEFT_WRITER_THREAD");
    if ( thread_create( NULL, writer_left_thread ) ) {
	TRACE(TRACE_ALWAYS, "thread_create() failed\n");
	QPIPE_PANIC();
    }


    // create WRITER_RIGHT
    tuple_buffer_t writer_right_buffer(sizeof(int));
    struct write_info_s writer_right_info = { &writer_right_buffer, count_right };
    tester_thread_t* writer_right_thread =
	new tester_thread_t(write_tuples, &writer_right_info, "RIGHT_WRITER_THREAD");
    if ( thread_create( NULL, writer_right_thread ) ) {
	TRACE(TRACE_ALWAYS, "thread_create() failed\n");
	QPIPE_PANIC();
    }
    


    // aggregate single count result (single int)
    tuple_buffer_t signal_buffer(sizeof(int));
    bnl_caching_join_packet_t* packet = 
	new bnl_caching_join_packet_t("BNL_CACHING_JOIN_PACKET",
				      &signal_buffer,
				      new tuple_filter_t(signal_buffer.tuple_size),
				      NULL, // tuple_join_t!
				      &signal_buffer,
				      &writer_left_buffer,
				      &writer_right_buffer);
    
    
    dispatcher_t::dispatch_packet(packet);
  
  
    tuple_t output;
    signal_buffer.init_buffer();
    TRACE(TRACE_ALWAYS, "get_tuple() returned %d\n",
	  signal_buffer.get_tuple(output) );
    TRACE(TRACE_ALWAYS, "TEST DONE\n");
    
    
    return 0;
}
