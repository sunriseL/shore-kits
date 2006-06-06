// -*- mode:C++; c-basic-offset:4 -*-

#include "stages/merge.h"
#include <list>

using std::list;

void merge_packet_t::terminate_inputs() {
    // TODO: close the input buffers
    for(buffer_list_t::iterator it=input_buffers.begin(); it != input_buffers.end(); ++it) {
        tuple_buffer_t *buf = *it;
        buf->close();
        // TODO: delete the buffer if the close fails
    }
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
merge_stage_t::buffer_head_t::buffer_head_t(tuple_buffer_t *buf,
                                            tuple_comparator_t *c) {
    buffer = buf;
    cmp = c;
    buffer->init_buffer();
        
}


bool merge_stage_t::buffer_head_t::has_next() {
    tuple_t tuple;
    if(!buffer->get_tuple(tuple))
        return false;

    // update the head
    item.key = cmp->make_key(tuple);
    item.data = tuple.data;
    return true;
}



/**
 * @brief Inserts an item into the list in ascending order
 */
void merge_stage_t::insert_sorted(const buffer_head_t &head)
{
    // find the right position
    head_list_t::iterator it=_buffers.begin();
    while(it != _buffers.end() && _comparator->compare(it->item, head.item) < 0)
        ++it;
    
    // insert
    _buffers.insert(it, head);
}

const char *merge_packet_t::PACKET_TYPE = "Merge";
const char *merge_stage_t::DEFAULT_STAGE_NAME = "Merge";

int merge_stage_t::process_packet() {
    merge_packet_t *packet = (merge_packet_t *)_adaptor->get_packet();

    typedef merge_packet_t::buffer_list_t buffer_list_t;
    typedef buffer_list_t::iterator iterator;

    _comparator = packet->comparator;
    size_t tuple_size = packet->output_buffer->tuple_size;
    buffer_list_t &inputs = packet->input_buffers;
    
    // get the input buffers and perform the initial sort
    for(iterator it=inputs.begin(); it != inputs.end(); ++it) {
        buffer_head_t head(*it, _comparator);
        if(head.has_next())
            insert_sorted(head);
    }

    // always output the smallest tuple
    tuple_t tuple(NULL, tuple_size);
    while(_buffers.size()) {
        buffer_head_t head = _buffers.front();
        _buffers.pop_front();
        tuple.data = head.item.data;
        if(_adaptor->output(tuple))
            return 1;

        // put it back?
        if(head.has_next())
            insert_sorted(head);
    }

    // done!
    return 0;
}
