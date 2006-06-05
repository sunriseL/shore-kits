/* -*- mode:C++; c-basic-offset:4 -*- */

#include "thread.h"
#include "stage_container.h"
#include "stages/fdump.h"
#include "trace/trace.h"
#include "qpipe_panic.h"

#include "tester_thread.h"



using namespace qpipe;



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
    tuple_buffer_t* int_buffer = (tuple_buffer_t*)arg;

    int_buffer->wait_init();

    
    for (int i = 0; i < 1000; i++) {
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



int main() {

    thread_init();
    
    
    stage_container_t* sc = new stage_container_t("FDUMP_CONTAINER", new stage_factory<fdump_stage_t>);
    tester_thread_t* fdump_thread = 
	new tester_thread_t(drive_stage, sc, "FDUMP_THREAD");
    
    if ( thread_create( NULL, fdump_thread ) ) {
	TRACE(TRACE_ALWAYS, "thread_create() failed\n");
	QPIPE_PANIC();
    }
    
    
    
    // just need to pass one int at a time to the counter
    tuple_buffer_t int_buffer(sizeof(int));
    tester_thread_t* writer_thread =
	new tester_thread_t(write_tuples, &int_buffer, "WRITER_THREAD");
    
    if ( thread_create( NULL, writer_thread ) ) {
	TRACE(TRACE_ALWAYS, "thread_create() failed\n");
	QPIPE_PANIC();
    }
    

    // aggregate single count result (single int)
    tuple_buffer_t  signal_buffer(sizeof(int));
    fdump_packet_t* packet = 
	new fdump_packet_t("FDUMP_PACKET", &signal_buffer, &int_buffer, "ints.bin");

    
    sc->enqueue(packet);
  
  
    tuple_t output;
    signal_buffer.init_buffer();
    TRACE(TRACE_ALWAYS, "get_tuple() returned %d\n",
	  signal_buffer.get_tuple(output) );
    TRACE(TRACE_ALWAYS, "TEST DONE\n");
    
    
    return 0;
}
