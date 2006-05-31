
#include "thread.h"
#include "tester_thread.h"
#include "stages/aggregate.h"
#include "trace/trace.h"
#include "qpipe_panic.h"

using namespace qpipe;



/**
 *  @brief Simulates a worker thread on the specified stage.
 *
 *  @param arg A stage_t* to work on.
 */
void* drive_stage(void* arg)
{
  stage_t *stage = (stage_t *)arg;
  while(1) {
    stage->process_next_packet();
  }
  return NULL;
}




void* write_tuples(void* arg)
{
  tuple_buffer_t *buffer = (tuple_buffer_t *)arg;

  // wait for the parent to wake me up...
  buffer->wait_init();
    
  int value;
  tuple_t input((char *)&value, sizeof(int));
  for(int i=0; i < 10; i++) {
    value = i;
    if(buffer->put_tuple(input))
    {
      TRACE(TRACE_ALWAYS, "buffer->put_tuple() returned non-zero!\n");
      TRACE(TRACE_ALWAYS, "Terminating loop...\n");
      break;
    }
    else
      TRACE(TRACE_ALWAYS, "Inserted tuple %d\n", value);
  }

  buffer->send_eof();
  return NULL;
}




class count_aggregate_t : public tuple_aggregate_t {

private:
  int count;
    
public:
  
  count_aggregate_t() {
    count = 0;
  }
  
  virtual bool eof(tuple_t &dest) {
    break_group(dest);
    return true;
  }
protected:

  void break_group(tuple_t &dest) {
    *(int*)dest.data = count;
  }

  virtual bool filter(tuple_t &, const tuple_t &) {
    // ignore group breaks for now
    count++;
    return false;
  }
};


int main() {

  thread_init();

  aggregate_stage_t *stage = new aggregate_stage_t();
  tester_thread_t* aggregate_thread = new tester_thread_t(drive_stage, stage, "AGGREGATE THREAD");
  if ( thread_create( NULL, aggregate_thread ) ) {
    TRACE(TRACE_ALWAYS, "thread_create failed\n");
    QPIPE_PANIC();
  }


  // for now just aggregate straight tuples
  tuple_buffer_t  input_buffer(sizeof(int));
  tester_thread_t* writer_thread = new tester_thread_t(write_tuples, &input_buffer, "WRITER THREAD");
  if ( thread_create( NULL, writer_thread ) ) {
    TRACE(TRACE_ALWAYS, "thread_create failed\n");
    QPIPE_PANIC();
  }

   
  // the output is always a single int
  tuple_buffer_t  output_buffer(sizeof(int));
  tuple_filter_t* filter = new tuple_filter_t();
  count_aggregate_t*  aggregator = new count_aggregate_t();
  aggregate_packet_t* packet = new aggregate_packet_t(NULL,
						      "test aggregate",
						      &output_buffer,
						      &input_buffer,
						      aggregator,
						      filter);

  stage->enqueue(packet);
    
  tuple_t output;
  output_buffer.init_buffer();
  while(output_buffer.get_tuple(output))
    TRACE(TRACE_ALWAYS, "Count: %d\n", *(int*)output.data);

  return 0;
}
