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
bool merge_stage_t::buffer_head_t::init(tuple_buffer_t *buf,
                                        tuple_comparator_t *c)
{
    buffer = buf;
    cmp = c;
    buffer->init_buffer();
    int size = buffer->tuple_size;
    data = new char[size];
    tuple = tuple_t(data, size);
    item.data = data;
    return has_tuple();
}

bool merge_stage_t::buffer_head_t::has_tuple() {
    tuple_t input;
    if(!buffer->get_tuple(input))
        return false;

    // copy the tuple to safe memory
    tuple.assign(input);

    // update the head key
    item.key = cmp->make_key(tuple);
    return true;
}



/**
 * @brief Inserts an item into the list in ascending order
 */
void merge_stage_t::insert_sorted(buffer_head_t *head)
{
    // beginning? (if so we have to change the list base pointer)
    if(!_head_list || _comparator->compare(head->item, _head_list->item) <= 0) {
        head->next = _head_list;
        _head_list = head;
        return;
    }

    // find the position, then
    buffer_head_t *prev = _head_list;
    while(prev->next && _comparator->compare(prev->next->item, head->item) < 0)
        prev = prev->next;
    
    // insert
    head->next = prev->next;
    prev->next = head;
}

const char *merge_packet_t::PACKET_TYPE = "MERGE";
const char *merge_stage_t::DEFAULT_STAGE_NAME = "MERGE_STAGE";


stage_t::result_t merge_stage_t::process_packet() {

    merge_packet_t *packet = (merge_packet_t *)_adaptor->get_packet();

    typedef merge_packet_t::buffer_list_t buffer_list_t;

    _comparator = packet->comparator;
    buffer_list_t &inputs = packet->input_buffers;
    
    // allocate an array of buffer heads
    int merge_factor = inputs.size();
    TRACE(TRACE_DEBUG, "merge factor = %d\n", merge_factor);
    buffer_head_t head_array[merge_factor];
                             
    // get the input buffers and perform the initial sort
    for(int i=0; i < merge_factor; i++) {
        buffer_head_t &head = head_array[i];
        if(head.init(inputs[i], _comparator))
            insert_sorted(&head);
    }

    // always output the smallest tuple
    for(int i=0; _head_list; i++) {
        // pop it off
        buffer_head_t *head = _head_list;
        _head_list = head->next;

        // output it
        result_t result = _adaptor->output(head->tuple);
	if (result)
	    return result;

        // put it back?
        if(head->has_tuple())
            insert_sorted(head);
    }

    // done!
    return stage_t::RESULT_STOP;
}
