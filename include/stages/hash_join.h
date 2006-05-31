/* -*- mode:C++; c-basic-offset:4 -*- */

/* hash_join_stage.h */
/* Declaration of the hash_join_stage hash_join_packet classes */
/* History: 
   3/3/2006: Removed the static hash_join_cnt variable of the hash_join_packet_t class, instead it  uses the singleton stage_packet_counter class.
*/


# ifndef __HASH_JOIN_STAGE_H
# define __HASH_JOIN_STAGE_H

# include "thread.h"
# include <ext/hash_set>
# include "stage.h"


using namespace qpipe;

/********************
 * hash_join_packet *
 ********************/

class hash_join_packet_t : public packet_t {
public:

  int hash_join_id;

  join_functor_t* join_func;
  
  size_t join_buffer_size;
  int num_partitions;	// number of partitions in hybrid hash-join
  
  tuple_buffer_t* input_buffer1;
  tuple_buffer_t* input_buffer2;
  
  int join_attribute_offset1;
  int join_attribute_offset2;
    // field_count | offset0 | offset1 | offset2 | ... | offsetN-1 |
  int join_length;

  bool equality_test;  // true: join tuples with equal keys. false: join tuples with not equal keys (e.g., NOT IN )
  
  int count_out;
  int count_left;
  int count_right;
  
  hash_join_packet_t(DbTxn* tid,
                     char* packet_id,
                     tuple_buffer_t* out_buffer,
                     tuple_buffer_t* in_buffer1,
                     tuple_buffer_t* in_buffer2,
                     int join_attr_offset1,
                     int join_attr_offset2,
                     size_t buffer_size,
                     int n_partitions,
                     join_functor_t* func,
                     bool equality_test_flag = true,
                     int length = 1);
  
  ~hash_join_packet_t();
  
  virtual bool mergable(stage_packet_t*);  
  virtual void link_packet(stage_packet_t* packet);
};


struct int_hasher {
  size_t operator()(const int& x) { return (size_t)x; }
};


struct int_equal {
  bool operator()(const int& x, const int& y) { return x == y; }
};



/*******************
 * hash_join_stage *
 *******************/


class hash_join_stage : public qpipe_stage {
  typedef hash_table<int, char*, int_hasher, int_equal > tuple_hash_t;
  
    /* Reads tuples from the given source to build an in-memory hash table
     */
    template<class TupleSource>
    page_t *build_hash(tuple_hash_t &probe_table,
                       TupleSource source);
    
    template <class TupleSource>
    hash_join_packet_t *probe_matches(tuple_hash_t &probe_table,
                                      hash_join_packet_t *packet,
                                      TupleSource source);
    
public:

  bool is_runnable();
  void enqueue(stage_packet_t* packet);
  int dequeue();

  int count_out;
  int count_left;
  int count_right;

  stage_queue_t* side_queue;

  int join_partition(int partition_num, hash_table<int, tuple_t*, int_hasher, int_equal > &probe_table, hash_join_packet_t* packet, int left_tuple_size, int right_tuple_size);

  int gen_multi_key(int* keyloc, int keylen);

  int free_probe_table(tuple_hash_t &probe_table);

  int cleanup(tuple_hash_t &probe_table, hash_join_packet_t* packet);

  hash_join_packet_t* process_tuple(const tuple_t* left_tuple,
                                    const tuple_t* right_tuple,
                                    hash_join_packet_t* packet);

  hash_join_stage(const char* sname) : qpipe_stage(sname) {
    side_queue = new stage_queue_t(sname);
    count_out = 0; 
    count_left = 0; 
    count_right = 0;

    stage_type = STAGE_HASH_JOIN;
  }

  ~hash_join_stage() {

    delete side_queue;
  }
};

#endif	// __HASH_JOIN_H
