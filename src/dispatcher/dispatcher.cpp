
#include "dispatcher/dispatcher.h"
#include "util/static_hash_map.h"
#include "util/hash_functions.h"
#include "trace/trace.h"
#include "qpipe_panic.h"

#include <cstdio>
#include <cstring>



using namespace qpipe;



/* internal helper functions */

int string_comparator (const void* key1, const void* key2);
size_t packet_type_hash(const void* key);



dispatcher_t::dispatcher_t() {
  static_hash_map_init( &stage_directory,
			&stage_directory_buckets[0],
			DISPATCHER_NUM_STATIC_HASH_MAP_BUCKETS,
			packet_type_hash,
			string_comparator);
}



dispatcher_t::~dispatcher_t() {
  TRACE(TRACE_ALWAYS, "Need to destroy nodes and keys\n");
}



int dispatcher_t::do_register_stage(const char* packet_type, stage_t* stage) {

  // allocate hash node and copy of key string
  static_hash_node_t node = (static_hash_node_t)malloc(sizeof(*node));
  if ( node == NULL ) {
    TRACE(TRACE_ALWAYS, "malloc() failed on static_hash_node_t\n");
    return -1;
  }

  char* ptcopy;
  if ( asprintf(&ptcopy, "%s", packet_type) == -1 ) {
    TRACE(TRACE_ALWAYS, "asprintf() failed\n");
    free(node);
    return -1;
  }
  
  // add to hash map
  static_hash_map_insert( &stage_directory, ptcopy, stage, node );
  return 0;
}


int dispatcher_t::register_stage(const char* packet_type, stage_t* stage) {
  return instance()->do_register_stage(packet_type, stage);
}





/* definitions of internal helper functions */

int string_comparator(const void* key1, const void* key2) {
  const char* str1 = (const char*)key1;
  const char* str2 = (const char*)key2;
  return strcmp(str1, str2);
}

size_t packet_type_hash(const void* key) {
  const char* str = (const char*)key;
  return (size_t)RSHash(str, strlen(str));
}
