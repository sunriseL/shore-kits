// -*- mode:C++; c-basic-offset:4 -*-

#include "sort.h"

void sort_stage_t::terminate() {
    // close the input buffer
    input_buffer->close();
}

struct page_list_guard_t {
    page_t *_head;
    page_list_guard_t(page_t *head)
        : _head(head)
    {
    }
    ~page_list_guard_t() {
        while(_head) {
            page_t *page = _head;
            _head = page->next;
            free(page);
        }
    }
};



int sort_stage_t::process_packet(packet_t *p) {
    sort_packet_t *packet = (sort_packet_t *)p;

    // automatically close the input buffer when this function exits
    buffer_closer_t input = packet->input_buffer;
    tuple_comparator_t *compare = packet->compare;

    // TODO: use (dynamically chosen) longer runs once we're sure this
    // code is debugged
    int page_count = 3;
    page_t *page_list = NULL;
    for(int i=0; i < page_count; i++) {
        page_t *page = tuple_page_t::alloc(input->tuple_size, malloc);
        page->next = page_list;
        page_list = page;
    }

    // free the page list when this function returns
    page_list_guard_t guard(page_list);

    // create all sorted runs
    while(!input->eof()) {
        // read in a run of pages

        // sort them

        // package them up into an fdump request
    }
    
    // TODO: short-cut the fdump if there is only one run

    // set up a hierarchical N-way merge
    int merged_runs = 0;
    do {
        while(1/* more sets of N sorted runs */) {
            // join on the first N fdumps

            // create N fscans and feed them to an N-way merge

            // add the merge to the list
        }
    } while(merged_runs > 1);

    // transfer the final sorted run into the output buffer
}
