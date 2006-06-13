/* -*- mode:C++; c-basic-offset:4 -*- */

#include "thread.h"
#include "tuple.h"



// include me last!
#include "namespace.h"



/**
 *  @brief If the current write page is full, try to write it to the
 *  internal page_buffer_t and allocate a new page. If this buffer is
 *  full (if it already has the maximum number of pages allocated),
 *  this function will block. If the consumer has closed the buffer If
 *  the page_buffer_t is full, this function will block.
 */

int tuple_buffer_t::check_page_full() {
    // next page?
    if(write_page->full()) {
        // cancelled?
        if(flush_write_page())
            return 1;
        
        // TODO: something besides malloc
        write_page = tuple_page_t::alloc(tuple_size, malloc, _page_size);
	return (write_page == NULL);
    }

    // otherwise just test for cancellation
    return page_buffer.stopped_reading();
}



/**
 *  @brief Block until either inputs become available or the
 *  producer closes the buffer (sending EOF).
 *
 *  @return 1 if the producer has closed the buffer. 0
 *  otherwise. If this function returns 0, inputs are
 *  available.
 */

int tuple_buffer_t::wait_for_input() {
 
    if(read_page != NULL && read_iterator != read_page->end())
	return 0;
   
    // wait for the next page to arrive
    page_t *page = page_buffer.read();
    if(!page)
        return 1;
        
    read_page = tuple_page_t::mount(page);
    read_iterator = read_page->begin();
    return 0;
}



/**
 *  @brief Retrieve a tuple from the buffer. This function blocks
 *  the calling thread if the buffer is empty.
 *
 *  @param tuple On success, this tuple is set to point to the
 *  tuple most recently removed from this page.
 *
 *  @return true if a tuple is successfully removed from the
 *  buffer. false if the buffer is empty and the producer has
 *  closed its end.
 */

bool tuple_buffer_t::get_tuple(tuple_t &rec) {

    // make sure there is a valid page
    if(wait_for_input())
        return false;
    
    rec = *read_iterator++;
    return true;
}



/**
 *  @brief Signal the consumer that no more tuples will be
 *  inserted into the buffer. Should only be invoked by the
 *  producer for this buffer. Once the tuples currently in the
 *  buffer are read, future read operations will fail.
 */

void tuple_buffer_t::send_eof() {
    if(!write_page->empty()) 
        flush_write_page();

    page_buffer.stop_writing();
}



/**
 *  @brief Close this tuple_buffer_t. Should only be invoked by
 *  the consumer for this buffer. Future tuple insertions will
 *  fail.
 */

void tuple_buffer_t::close() {
    page_buffer.stop_reading();
}



/**
 *  @brief Initialize this page buffer with a single empty tuple
 *  page.
 *
 *  @param _tuple_size The size of the tuples this buffer will store.
 *
 *  @param page_size The size of the pages to use for this buffer.
 *
 *  @return 0 on successful initialization. Negative value on error
 *  (if the allocate fails to create a page).
 */

int tuple_buffer_t::init(size_t _tuple_size, size_t page_size) {

    write_page = tuple_page_t::alloc(_tuple_size, malloc, page_size);
    if (write_page == NULL)
	return -1;
	
    input_arrived = false;
    initialized = false;
    tuple_size = _tuple_size;
    _page_size = page_size;
    read_page = NULL;

    pthread_mutex_init_wrapper(&init_lock, NULL);
    pthread_cond_init_wrapper(&init_notify, NULL);
    return 0;
}



/**
 *  @brief tuple_buffer_t destructor.
 */

tuple_buffer_t::~tuple_buffer_t() {

    pthread_mutex_destroy_wrapper(&init_lock);
    pthread_cond_destroy_wrapper(&init_notify);
}



#include "namespace.h"
