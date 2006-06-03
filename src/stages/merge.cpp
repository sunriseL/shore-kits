// -*- mode:C++; c-basic-offset:4 -*-

#include "stages/merge.h"
#include <list>

using std::list;

void merge_packet_t::terminate() {
    // TODO: close the input buffers
}

/**
 * This merge stage maintains a linked list of input buffers, sorted
 * by the next data item in each buffer, in order to choose the next
 * buffer to read from.
 *
 * If merges of more than 8-10 runs are common the code should be
 * extended to use a modified heap (see note below) as well as -- or
 * instead of -- the list.
 *
 * A 7-way merge using a list requires 7/2 = 3.5 and < 1 comparisons
 * and swaps, respectively; a 15-way merge requires 7.5 and < 1
 * comparisons and swaps on average.
 *
 * Using a heap for a 7-way merge one can expect to make 2*(1*1 +
 * 2*6)/7 = 3.7 comparisons and (0*1 + 1*2 + 2*4)/7 = 1.4 swaps per
 * tuple read; a 15-way merge requires 2*(1*1 + 2*2 + 3*12)/15 = ~5.5
 * comparisons and (0*1 + 1*2 + 2*4 + 3*8)/15 = ~2.3 swaps, on
 * average.
 *
 * NOTE: merging is a special case operation where we mutate the 'min'
 * element rather than removing it. Most generic data structures would
 * require removing and reinserting the element to maintain proper
 * ordering. This is fine lists where the remove operation trivial,
 * but would increase the number of comparisons and swaps required for
 * the heap by up to 2x and make it even less attractive than shown
 * above.
 */
struct buffer_head_t {
    tuple_buffer_t *buffer;
    tuple_comparator_t *cmp;
    key_tuple_pair_t item;
    buffer_head_t(tuple_buffer_t *buf, tuple_comparator_t *c) {
        buffer = buf;
        cmp = c;
        buffer->init_buffer();
        
    }
    bool has_next() {
        tuple_t tuple;
        if(!buffer->get_tuple(tuple))
            return false;

        // update the head
        item.key = cmp->make_key(tuple);
        item.data = tuple.data;
        return true;
    }
};

struct head_less_t {
    tuple_comparator_t *comparator;
    head_less_t(tuple_comparator_t *cmp) {
        comparator = cmp;
    }
    bool operator()(const buffer_head_t &a, const buffer_head_t &b) {
        return comparator->compare(a.item, b.item);
    }
};

typedef list<buffer_head_t> head_list_t;



/**
 * @brief Inserts an item into the list in ascending order
 */
static void insert_sorted(head_list_t &buffers,
                          const buffer_head_t &head,
                          tuple_comparator_t *comparator)
{
    // find the right position
    head_list_t::iterator it=buffers.begin();
    while(it != buffers.end() && comparator->compare(it->item, head.item) < 0)
        ++it;
    
    // insert
    buffers.insert(it, head);
}

const char *merge_packet_t::TYPE = "Merge";
const char *merge_stage_t::NAME = "Merge";

int merge_stage_t::process_packet(packet_t *p) {
    merge_packet_t *packet = (merge_packet_t *)p;

    typedef merge_packet_t::buffer_list_t buffer_list_t;
    typedef buffer_list_t::iterator iterator;

    tuple_comparator_t *comparator = packet->comparator;
    size_t tuple_size = packet->output_buffer->tuple_size;
    buffer_list_t &inputs = packet->input_buffers;
    
    // get the input buffers and perform the initial sort
    head_list_t buffers;
    for(iterator it=inputs.begin(); it != inputs.end(); ++it) {
        buffer_head_t head(*it, comparator);
        if(head.has_next())
            insert_sorted(buffers, head, comparator);
    }

    // always output the smallest tuple
    tuple_t tuple(NULL, tuple_size);
    while(buffers.size()) {
        buffer_head_t head = buffers.front();
        buffers.pop_front();
        tuple.data = head.item.data;
        if(output(packet, tuple))
            return 1;

        // put it back?
        if(head.has_next())
            insert_sorted(buffers, head, comparator);
    }

    // done!
    return 0;
}
