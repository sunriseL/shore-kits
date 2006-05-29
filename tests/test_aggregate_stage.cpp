#include "stages/aggregate.h"
#include "trace/trace.h"

using namespace qpipe;

void *drive_stage(void *arg) {
    stage_t *stage = (stage_t *)arg;
    while(1) {
        stage->process_next_packet();
    }

    return NULL;
}

void *write_tuples(void *arg) {
    tuple_buffer_t *buffer = (tuple_buffer_t *)arg;

    // wait for the parent to wake me up...
    buffer->wait_init();
    
    int value;
    tuple_t input((char *)&value, sizeof(int));
    for(int i=0; i < 10; i++) {
        value = i;
        if(buffer->put_tuple(input)) {
            TRACE(TRACE_ALWAYS, "Early termination!");
            break;
        }
    }

    buffer->send_eof();
    return NULL;
}

class count_aggregate_t : public tuple_aggregate_t {
private:
    int count;
    
public:
    virtual void eof(tuple_t &dest) {
        break_group(dest);
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
    aggregate_stage_t *stage = new aggregate_stage_t();

    pthread_t stage_thread;
    pthread_t source_thread;
    
    pthread_create(&stage_thread, NULL, &drive_stage, stage);

    // for now just aggregate straight tuples
    tuple_buffer_t input_buffer(sizeof(int));
    pthread_create(&source_thread, NULL, &write_tuples, &input_buffer);
    
    // the output is always a single int
    tuple_buffer_t output_buffer(sizeof(int));

    tuple_filter_t *filter = new tuple_filter_t();
    count_aggregate_t *aggregator = new count_aggregate_t();
    
    aggregate_packet_t *packet = new aggregate_packet_t(NULL,
                                                        "test aggregate",
                                                        &output_buffer,
                                                        &input_buffer,
                                                        aggregator,
                                                        filter);

    stage->enqueue(packet);
    
    tuple_t output;
    output_buffer.init_buffer();
    while(output_buffer.get_tuple(output))
        printf("Count: %d\n", *(int*)output.data);

    return 0;
}
