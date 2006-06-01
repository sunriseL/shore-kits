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
        if(page_buffer.write(write_page))
            return 1;
        
        // TODO: something besides malloc
        write_page = tuple_page_t::alloc(tuple_size, malloc);
        return 0;
    }

    // otherwise just test for cancellation
    return page_buffer.stopped_reading();
}



/**
 *  @brief Block until either inputs become available or the
 *  producer closes the buffer (sending EOF).
 *
 *  @return false if the producer has closed the buffer. true
 *  otherwise. If this function returns true, inputs are
 *  available.
 */

int tuple_buffer_t::wait_for_input() {
 
    if(read_page != NULL)
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
    if(read_iterator == read_page->end()) {
        free(read_page);
        read_page = NULL;
    }
    
    return true;
}



/**
 *  @brief Signal the consumer that no more tuples will be
 *  inserted into the buffer. Should only be invoked by the
 *  producer for this buffer. Once the tuples currently in the
 *  buffer are read, future read operations will fail.
 */

void tuple_buffer_t::send_eof() {
    if(!write_page->empty()) {
        page_buffer.write(write_page);
        write_page = NULL;
    }

    page_buffer.stop_writing();
}



/**
 *  @brief Close this tuple_buffer_t. Should only be invoked by
 *  the consumer for this buffer. Future tuple insertions will
 *  fail.
 */

void tuple_buffer_t::close() {
    free(read_page);
    page_buffer.stop_reading();
}



/**
 *  @brief Initialize this page buffer with a single empty tuple
 *  page.
 *
 *  @param _tuple_size The size of the tuples this buffer will store.
 *
 *  @param _page_size The size of the pages to use for this buffer.
 *
 *  @return 0 on successful initialization. Negative value on error
 *  (if the allocate fails to create a page).
 */

int tuple_buffer_t::init(size_t _tuple_size, size_t _page_size) {

    write_page = tuple_page_t::alloc(_tuple_size, malloc);
    if (write_page == NULL)
	return -1;
	
    input_arrived = false;
    initialized = false;
    tuple_size = _tuple_size;
    page_size = _page_size;
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

    // The page_buffer_t destructor will destroy its pages. We need to
    // free the read and write page separately since they are outside
    // the buffer.
    if(read_page)
        free(read_page);
    if(write_page)
	free(write_page);
}



/**
 *  @brief Called by consumer to initiate the buffer. This function
 *  unlocks the buffer and lets any thread(s) waiting on wait_init()
 *  return so they can start inserting tuples.
 */

void tuple_buffer_t::init_buffer() {
    
    // * * * BEGIN CRITICAL SECTION * * *
    critical_section_t cs(&init_lock);
    
    // unblock waiting producer
    initialized = true;
    pthread_cond_signal_wrapper(&init_notify);
    // * * * END CRITICAL SECTION * * *
}



/**
 *  @brief Called by the producer to wait until the consumer initiates
 *  the communication buffer. If block is true, this function waits
 *  for the buffer's output stage to initialize and unlock the
 *  buffer. Otherwise, it simply checks whether the buffer is
 *  initialized.
 *
 *  @param block If the buffer is not initialized, this parameter
 *  determines whether we will wait for it to become initialized.
 *
 *  @return If block is false, this function will return whether the
 *  buffer is initialized. If block is true, this function will return
 *  0 when the consumer is ready to receive data. If block is true,
 *  this function will return non-zero if the consumer has closed its
 *  end of the buffer.
 */

int tuple_buffer_t::wait_init(bool block) {
    
    // * * * BEGIN CRITICAL SECTION * * *
    critical_section_t cs(&init_lock);

    if(!block) 
        return initialized? 0 : 1;
    
    while(!initialized && !page_buffer.stopped_reading())
        pthread_cond_wait_wrapper(&init_notify, &init_lock);

    return page_buffer.stopped_reading()? -1 : 0;
    // * * * END CRITICAL SECTION * * *
}



#include "namespace.h"
