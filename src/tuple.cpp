#include "tuple.h"
#include "thread.h"

// include me last!
#include "namespace.h"

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

void tuple_buffer_t::send_eof() {
    if(!write_page->empty()) {
        page_buffer.write(write_page);
        write_page = NULL;
    }

    page_buffer.stop_writing();
}

void tuple_buffer_t::close() {
    free(read_page);
    page_buffer.stop_reading();
}

void tuple_buffer_t::init(size_t _tuple_size, size_t _page_size) {
    input_arrived = false;
    initialized = false;
    tuple_size = _tuple_size;
    page_size = _page_size;
    read_page = NULL;
    write_page = tuple_page_t::alloc(_tuple_size, malloc);
    pthread_mutex_init_wrapper(&init_lock, NULL);
    pthread_cond_init_wrapper(&init_notify, NULL);
}

tuple_buffer_t::~tuple_buffer_t() {
    pthread_mutex_destroy_wrapper(&init_lock);
    pthread_cond_destroy_wrapper(&init_notify);
    if(read_page)
        free(read_page);
    if(write_page)
        free(write_page);
        
}

/**
 * Unlock the buffer and lets those that are waiting for it to start consuming
 * a thread can wait for a buffer of tuples by calling the wait_init  
 */

void tuple_buffer_t::init_buffer() {
    critical_section_t cs(&init_lock);
    
    // unblock waiting producer
    initialized = true;
    pthread_cond_signal_wrapper(&init_notify);
    cs.exit();
}

/**
 * @brief waits for the buffer's output stage to unlock the buffer
 */
int tuple_buffer_t::wait_init(bool block) {
    critical_section_t cs(&init_lock);

    if(!block) 
        return cs.exit(initialized? 0 : 1);
    while(!initialized && !page_buffer.stopped_reading())
        pthread_cond_wait_wrapper(&init_notify, &init_lock);

    return cs.exit(page_buffer.stopped_reading()? -1 : 0);
}


#include "namespace.h"
