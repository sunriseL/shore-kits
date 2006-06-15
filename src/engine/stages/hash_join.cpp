
/* hash_join_stage.cpp */
/* Implementation of the HASH_JOIN operator */
/* History: 
   3/6/2006: Uses the outtup variable of the output_tup_t, instead of the data.
*/


#include "stages/hash_join.h"
#include "functors.h"

/**********************
 * hash_join_packet_t *
 **********************/

hash_join_packet_t::hash_join_packet_t(DbTxn*          tid,
                                       char*           packet_id,
                                       tuple_buffer_t* out_buffer,
                                       tuple_buffer_t* left_buffer,
                                       tuple_buffer_t* right_buffer,
                                       tuple_join_t*   join,
                                       size_t          buffer_size,
                                       int             n_partitions,
                                       join_functor_t* func,
                                       bool            equality_test_flag,
                                       int             length)
  : stage_packet_t(tid, packet_id, out_buffer)
{
  input_buffer1 = in_buffer1;
  input_buffer2 = in_buffer2;

  join_attribute_offset1 = join_attr_offset1;
  join_attribute_offset2 = join_attr_offset2;
  TRACE_DEBUG("Hash Join packet=%s join_attribute_offset1=%d join_attribute_offset2=%d\n", packet_id, join_attribute_offset1, join_attribute_offset2);

  join_buffer_size = buffer_size;
  num_partitions = n_partitions;

  join_func = func;
  join_length = length;

  equality_test = equality_test_flag;

  stage_packet_counter * spc = stage_packet_counter::instance();
  spc->get_next_packet_id(&hash_join_id);
}

  
hash_join_packet_t::~hash_join_packet_t()
{
}



/*******************
 * hash_join_stage *
 *******************/

bool hash_join_stage::is_runnable()
{
  return true;
}

bool hash_join_packet_t::mergable(stage_packet_t* packet)
{
  return false; /* disable merging at sort for now */ 

  if (!strcmp(packet->packet_string_id, packet_string_id)) {
    return true;
  }

  return false; 
}



// we want to redifine link packet for hash_join stage
// to make sure that merged packet properly
// closes its producers

void hash_join_packet_t::link_packet(stage_packet_t* packet)
{
	// perform the usual linking
	stage_packet_t::link_packet(packet);
	((hash_join_packet_t*)packet)->input_buffer1->close_buffer();
	((hash_join_packet_t*)packet)->input_buffer2->close_buffer();
	((hash_join_packet_t*)packet)->hash_join_id = hash_join_id;
	((hash_join_packet_t*)packet)->num_partitions = num_partitions;
}


void hash_join_stage::enqueue(stage_packet_t* packet)
{
  // scan the stage queue or side queue to see if there is a packet 
  // that can be merged
  if(!queue->find_and_link_mergable(packet) && !side_queue->find_and_link_mergable(packet)) {
    TRACE_MONITOR("|%d|P=%d;S=%d", MSG_ENQUEUE_PACKET, packet->unique_id, STAGE_HASH_JOIN);
    queue->insert(packet);		
  }

  else {
    TRACE_MONITOR("|%d|S=%d;TH=%x;MSG=STAGE: %d, HASH_JOIN_STAGE: Worksharing Opportunity!! Packet=%d|\n",
                  MSG_STAGE_MESSAGE,
                  STAGE_HASH_JOIN,
                  (uint)pthread_self(),
                  STAGE_HASH_JOIN,
                  packet->unique_id);    

    TRACE_DEBUG("HASH_JOIN_STAGE: WORK SHARING OPPORTUNITY %d\n", packet->unique_id);
  }

}

struct tuple_source {
    size_t _offset, _size;

    // offset is in bytes, size is in ints
    tuple_source(size_t attr_offset, size_t attr_size)
        : _offset(attr_offset), _size(attr_size)
    {
    }

    int gen_key(char *data) {
        int *keyloc = (int *) &data[_offset];
# if defined(__GNUC__) && defined(sparc)
        int res;
        memcpy(&res, keyloc, sizeof(int));
# else
        int res = *(int*)keyloc;
# endif

        while(_size > 1) {
            keyloc++;
            _size--;
            res = res * 2;
            res += *keyloc;
        }
        return res;
    }

};

struct first_pass_tuple_source : public tuple_source {
    tuple_buffer_t *_buffer;
    int *_count;
    FILE **_files;
    tuple_page_t **_pages;
    int _num_partitions;
    first_pass_tuple_source(size_t attr_offset, size_t attr_size,
                            tuple_buffer_t *buffer, int *count,
                            FILE **files, tuple_page_t **pages,
                            int num_partitions)
        : tuple_source(attr_offset, attr_size),
          _buffer(buffer), _count(count),
          _files(files), _pages(pages),
          _num_partitions(num_partitions)
    {
    }
    
    // writes out a page to file
    void flush_page(int partition_num) {
        _pages[partition_num]->fwrite(_files[partition_num]);
        _pages[partition_num]->clear();
    }

    // return false if no entries were written
    bool fill_page(tuple_page_t *page) {
        tuple_t tuple;

        _pages[0] = page;
        while(!_buffer->get_tuple(tuple)) {
            int key = gen_key(tuple.data);
            int partition_num = key % _num_partitions;
            tuple_page_t *page = _pages[partition_num];

            ++*_count;
            
            // copy the tuple into the page
            page->append_init(tuple);
            if(page->full()) {
                if(partition_num == 0)
                    return true;

                flush_page(partition_num);
            }
        }

        // Flush buffer pages to file
        for(int i=1; i < _num_partitions; i++) 
            flush_page(i);

        // all done!
        return !page->empty();
    }

    size_t tuple_size() {
        return _buffer->tuple_size;
    }

    void terminate() {
        _buffer->close_buffer();
    }
};

struct file_tuple_source : public tuple_source {
    FILE *_file;
    size_t _tuple_size;

    file_tuple_source(size_t attr_offset, size_t attr_size,
                      FILE *file, size_t tuple_size)
        : tuple_source(attr_offset, attr_size),
          _file(file), _tuple_size(tuple_size)
    {
    }

    bool fill_page(tuple_page_t *page) {
        return page->fread(_file);
    }

    size_t tuple_size() {
        return _tuple_size;
    }

    void terminate() { }
};

template<class TupleSource>
page_t *hash_join_stage::build_hash(tuple_hash_t &probe_table,
                                          TupleSource source)
{
    page_t *page_list=NULL;
    tuple_page_t *page = tuple_page_t::alloc(source.tuple_size(), malloc);
    while( source.fill_page(page) ) {
        for(tuple_page_t::iterator it = page->begin(); it != page->end(); ++it) {
            char *data = it->data;
            int key = source.gen_key(data);
            probe_table.insert(key, data);
        }

        // next!
        page->next = page_list;
        page_list = page;
        page = tuple_page_t::alloc(source.tuple_size(), malloc);
    }

    // free the memory we used
    // FIXME: dealloc the page list after using the hash table...
    free(page);

    return page_list;
}

template <class TupleSource>
hash_join_packet_t *
hash_join_stage::probe_matches(tuple_hash_t &probe_table,
                               hash_join_packet_t *packet,
                               TupleSource source) {

    bool equality_test = packet->equality_test;
    tuple_page_t *page = tuple_page_t::alloc(source.tuple_size(), malloc);
    bool early_termination = false;
    while( !early_termination && source.fill_page(page) ) {
        for(tuple_page_t::iterator it = page->begin(); it != page->end(); ++it) {
            const tuple_t &right_tup = *it;
            int key = source.gen_key(right_tup.data);
            tuple_hash_t::iterator iter = probe_table.find(key);

            if(iter == probe_table.end() && !equality_test)
                iter = probe_table.begin();
            
            while (iter != probe_table.end()) {
                if(equality_test && (*iter).first != key)
                    continue;
                
                tuple_t left_tup(source.tuple_size(), (*iter).second);
      
                // join two matching tuples
                packet = process_tuple(&left_tup, &right_tup, packet);
                if (!packet) {
                    // early termination by the parent
                    source.terminate();
                    break;
                }
                

                // advance the iterator
                if(equality_test)
                    iter = iter.next_dup();
                else
                    ++iter;
            }
        }

        // next!
        page->clear();
    }

    // done!
    free(page);
    return packet;
}

int hash_join_stage::dequeue()
{
    hash_join_packet_t* packet = (hash_join_packet_t*)queue->remove();

    if (packet == NULL) {
        if (DBG_MSG) fprintf(stderr, "[hash_join_stage::dequeue] NULL packet\n");
        return (-1);
    }  

    TRACE_MONITOR("|%d|P=%d;S=%d;TH=%x", MSG_DEQUEUE_PACKET, packet->unique_id, STAGE_HASH_JOIN, (uint)pthread_self());


    /* check for rebinding directions from the dispatcher */
    if ( packet->bind_cpu != NULL )
        dispatcher_cpu_bind_self( packet->bind_cpu );
  
  
    // wait till our parents allow us to proceed
    if (packet->output_buffer->wait_init() != 0) {
        packet->input_buffer1->close_buffer();
        packet->input_buffer2->close_buffer();

        delete packet;
        return (0);

    } else {
        packet->input_buffer1->init_buffer();
    }
  
    side_queue->insert(packet);


    hash_join_packet_t *temp_packet;
    int left_tuple_size = packet->input_buffer1->tuple_size;
    int right_tuple_size = packet->input_buffer2->tuple_size;
    char fname[512];
  
    // tuple counter
    packet->count_left = 0; packet->count_right = 0; packet->count_out = 0;
  
    // create hash table
    tuple_hash_t probe_table(packet->join_buffer_size
                             / sizeof(tuple_hash_t::hash_bucket));
  
    // open files to dump partitions into
    int num_partitions = packet->num_partitions;
    FILE *partition_files1[num_partitions];
    FILE *partition_files2[num_partitions];
    tuple_page_t* partition_pages[num_partitions];

    // if the left incoming buffer does not return any tuple
    if (!packet->input_buffer1->wait_for_input()) {
        packet->output_buffer->send_eof();

        // notify monitor that packet finished
        TRACE_MONITOR("|%d|P=%d;S=%d;TH=%x|", MSG_PACKET_FINISHED, packet->unique_id, STAGE_HASH_JOIN, (uint)pthread_self());

        delete packet;
        return (0);
    }
  
    // initialize the partitions now (index 0 always invalid!!!)
    for (int i = 1; i < num_partitions; i++) {
        sprintf(fname, "%s/hash%d_file1_part%d", TMP_DIR, packet->hash_join_id, i);
        partition_files1[i] = fopen(fname, "w");

        sprintf(fname, "%s/hash%d_file2_part%d", TMP_DIR, packet->hash_join_id, i);
        partition_files2[i] = fopen(fname, "w");

        // TODO: something besides malloc
        partition_pages[i] = tuple_page_t::alloc(left_tuple_size, malloc);
    }
  

    // partition left side and build partition 0 hash
    first_pass_tuple_source source1(packet->join_attribute_offset1,
                                    packet->join_length,
                                    packet->input_buffer1,
                                    &packet->count_left,
                                    partition_files1, partition_pages,
                                    num_partitions);
    page_t *page_list = build_hash(probe_table, source1);

    TRACE_DEBUG("Hash Join partitioned %u tuples to the left (%d partitions)\n",
                packet->count_left,
                packet->num_partitions);
  


    // reset pages for use with the right side
    for(int i=1; i < num_partitions; i++) 
        tuple_page_t::init(partition_pages[i], right_tuple_size);
  
    packet->input_buffer2->init_buffer();
  
    // remove ourselves from side queue - we are no longer mergable
    side_queue->remove(packet);
  
    // partition right side and join partition 0
    first_pass_tuple_source source2(packet->join_attribute_offset2,
                                    packet->join_length,
                                    packet->input_buffer2,
                                    &packet->count_right,
                                    partition_files2, partition_pages,
                                    num_partitions);
    temp_packet = probe_matches(probe_table, packet, source2);
  
    TRACE_DEBUG("Hash Join partitioned %u tuples to the right (%d partitions)\n",
                packet->count_right, packet->num_partitions);

    // free buffer pages
    while(page_list) {
        page_t *page = page_list;
        page_list = page_list->next;
        free(page);
    }
    
    // close partition files
    for (int i = 1; i < num_partitions; i++) {
        fclose(partition_files1[i]);
        fclose(partition_files2[i]);
        free(partition_pages[i]);
    }
  
    if (!temp_packet) {
        packet->input_buffer2->close_buffer();

        // Ryan: is this the right way to close the output buffer?
        // send result to the waiting (chained) packets
        process_tuple(NULL, NULL, packet);
        
        // notify monitor that packet finished
        TRACE_MONITOR("|%d|P=%d;S=%d;TH=%x|", MSG_PACKET_FINISHED,
                      packet->unique_id, STAGE_HASH_JOIN, (uint)pthread_self());
    
        delete packet;
        return 0;
    }

    packet = temp_packet;

    // join the remaining partitions
    for(int i=1; i < num_partitions; i++) {
        TRACE_DEBUG("Hash-Joining partition %d\n", i);
        
        probe_table.clear();
        
        sprintf(fname, "%s/hash%d_file1_part%d", TMP_DIR,
                packet->hash_join_id, i);
        FILE *file1 = fopen(fname, "r");
        
        sprintf(fname, "%s/hash%d_file2_part%d", TMP_DIR,
                packet->hash_join_id, i);
        FILE *file2 = fopen(fname, "r");
        
        file_tuple_source source1(packet->join_attribute_offset1,
                                  packet->join_length,
                                  partition_files1[i],
                                  left_tuple_size);
        page_list = build_hash(probe_table, source1);
      
        file_tuple_source source2(packet->join_attribute_offset2,
                                  packet->join_length,
                                  partition_files2[i],
                                  right_tuple_size);
        temp_packet = probe_matches(probe_table, packet, source2);
        fclose(file1);
        fclose(file2);

        // free buffer pages
        while(page_list) {
            page_t *page = page_list;
            page_list = page_list->next;
            free(page);
        }
    
        if(!temp_packet)
            break;

        packet = temp_packet;
    }
  
    // send result to the waiting (chained) packets
    process_tuple(NULL, NULL, packet);


  
    TRACE_DEBUG(".: TH%d Hash Join %s: |L|=%d, |R|=%d, |O|=%d\n",
                pthread_self(), packet->packet_string_id, packet->count_left,
                packet->count_right, packet->count_out);
    
    // remove partition files
    for (int i = 1; i < packet->num_partitions; i++) {
        sprintf(fname, "%s/hash%d_file1_part%d", TMP_DIR,
                packet->hash_join_id, i);
        remove(fname);
        sprintf(fname, "%s/hash%d_file2_part%d", TMP_DIR,
                packet->hash_join_id, i);
        remove(fname);
    }

    // notify monitor that packet finished
    TRACE_MONITOR("|%d|P=%d;S=%d;TH=%x|", MSG_PACKET_FINISHED, packet->unique_id,
                  STAGE_HASH_JOIN, (uint)pthread_self());

    delete packet;
    return 0;
}



hash_join_packet_t* hash_join_stage::process_tuple(const tuple_t *left_tup,
                                                   const tuple_t* right_tup,
                                                   hash_join_packet_t* packet)
{
  hash_join_packet_t* cur_packet = packet;
  hash_join_packet_t* prev_packet = NULL;
  hash_join_packet_t* temp_packet = NULL;
  
  if (!left_tup || !right_tup) {
    do {
        cur_packet->output_buffer->send_eof();
      temp_packet = cur_packet;
      cur_packet = (hash_join_packet_t*)cur_packet->next_packet;
      if (temp_packet != packet)
        delete temp_packet;
    } while (cur_packet);
    TRACE_DEBUG("hash_join_stage::process_tuple returned immediately\n");
    return NULL;
  }
  
  bool first_deleted = false;
  
  while (cur_packet) {	
    
      // join two matching tuples
      tuple_t out_tup;
      if (cur_packet->output_buffer->alloc_tuple(out_tup) != 0) {
          if (!prev_packet) {
              first_deleted = true;
              cur_packet = (hash_join_packet_t*)cur_packet->next_packet;
          } else {
              prev_packet->next_packet = cur_packet->next_packet;
              temp_packet = cur_packet;
              cur_packet = (hash_join_packet_t*)cur_packet->next_packet;
	  
              TRACE_MONITOR("|%d|P=%d;S=%d;TH=%x|", MSG_PACKET_FINISHED, temp_packet->unique_id, STAGE_HASH_JOIN, (uint)pthread_self());
	
              delete temp_packet;
          }							
          continue;
      }
      
      cur_packet->join_func->create_output_tuple(out_tup, *left_tup, *right_tup);
      packet->count_out++;
  }
  
  prev_packet = cur_packet;
  cur_packet = (hash_join_packet_t*)cur_packet->next_packet;

  
  if (first_deleted) {
    // check if there is anything left in a chain
    if (packet->next_packet) {
      temp_packet = packet;
      packet = (hash_join_packet_t*)packet->next_packet;

      TRACE_MONITOR("|%d|P=%d;S=%d;TH=%x|", MSG_PACKET_FINISHED, temp_packet->unique_id, STAGE_HASH_JOIN, (uint)pthread_self());

      delete temp_packet;
    } 
    else {
      // we are done with all the chained packets
      return NULL;
    }
  }
  
  return packet;
}
