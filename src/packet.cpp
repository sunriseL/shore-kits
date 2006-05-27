#include "stage.h"

#include "trace/trace.h"

// include me last!
#include "namespace.h"

/**
 * class packet_counter
 *
 * Singleton class that returns a unique identifier for all the packets 
 * in the system
 */

class packet_counter {
private:
  /* counter and mutex */
  int packet_cnt;
  pthread_mutex_t cnt_mutex;

  /* instance and mutex */
  static packet_counter* spcInstance;
  static pthread_mutex_t instance_mutex;

  packet_counter();
  ~packet_counter();

public:
  static packet_counter* instance();
  void get_next_packet_id(int* packet_id);
};


packet_t::packet_t(DbTxn* tid, char* packet_id, tuple_buffer_t* out_buffer) :
    xact_id(tid), packet_string_id(packet_id), output_buffer(out_buffer) {

    // mutex that protects the list of the linked packets for this packet
    pthread_mutex_init_wrapper(&mutex, NULL);
    next_packet = NULL;
    num_merged_packets = 1;

    // receives a unique id for the packet
    packet_counter * spc = packet_counter::instance();
    spc->get_next_packet_id(&unique_id);

}


packet_t::~packet_t() {

    pthread_mutex_destroy_wrapper(&mutex);
}

  
bool packet_t::mergable(packet_t* packet) { 
    // by default a packet is not mergable

    return false; 

    UNUSED( packet );
}
 

void packet_t::link_packet(packet_t* packet) {
    
    pthread_mutex_lock_wrapper(&mutex);

    // prepend new packet
    packet->next_packet = next_packet;
    next_packet = packet;
    num_merged_packets++;

    pthread_mutex_unlock_wrapper(&mutex);
}


packet_counter* packet_counter::spcInstance = NULL;

pthread_mutex_t packet_counter::instance_mutex = PTHREAD_MUTEX_INITIALIZER;


/*
 * Constructor: initializes the packet counter and its  mutex
 */

packet_counter::packet_counter() {

  if (pthread_mutex_init_wrapper(&cnt_mutex, NULL)) {
    TRACE(TRACE_ALWAYS, "Error initializing packet counter mutex\n");
  }

  packet_cnt = 0;
}


/*
 * instance(): Returns the unique instance of the class (singleton)
 */

packet_counter* packet_counter::instance() {

  // first check without concern of locking
  if ( spcInstance == NULL ) {

    // instance does not exist yet
    pthread_mutex_lock_wrapper(&instance_mutex);

    // we have the lock but another thread may have gotten the lock first
    if (spcInstance == NULL) {
      spcInstance = new packet_counter();
    }

    // release instance mutex
    pthread_mutex_unlock_wrapper(&instance_mutex);
  }

  return (spcInstance);
}


/*
 * Destructor destroys the mutex
 */

packet_counter::~packet_counter() {

  pthread_mutex_destroy_wrapper(&cnt_mutex);
  pthread_mutex_destroy_wrapper(&packet_counter::instance_mutex);
}


/*
 * get_next_packet_id(int*): Returns the current value of the packet counter and increases it
 */

void packet_counter::get_next_packet_id(int* packet_id) {
 
  pthread_mutex_lock_wrapper(&cnt_mutex);
  *packet_id = packet_cnt;
  TRACE(TRACE_ALWAYS, "New Packet: %d\n", packet_cnt);
  packet_cnt = (packet_cnt + 1) % (INT_MAX - 1);
  pthread_mutex_unlock_wrapper(&cnt_mutex);
}


#include "namespace.h"
