/* -*- mode:C++ c-basic-offset:4 -*- */
#include "thread.h"
#include "page.h"



// include me last!
#include "namespace.h"



/**
 *  @brief Initialize a page buffer (can be invoked by
 *  subclasses).
 *
 *  @param capacity The maximum number of pages to store in this
 *  buffer.
 *
 *  @param threshold If the buffer fills up, the producer will wait
 *  for it to contain at least this many free slots. If the buffer
 *  becomes empty, the consumer will wait for it to contain at least
 *  this many pages.
 */
void page_buffer_t::init(int capacity, int threshold) {

    page_buffer_t::capacity = capacity;
    if(threshold < 0)
        page_buffer_t::threshold = (int) (.3*capacity);
    else
        page_buffer_t::threshold = threshold;
    
    size = 0;

    // Pretend that we have a page already in the buffer. The consumer
    // won't try to remove it until the buffer is closed.
    read_size_guess  = 1;
    write_size_guess = 0;

    head = tail = NULL;
    done_reading = done_writing = false;

    pthread_mutex_init_wrapper(&lock, NULL);
    pthread_cond_init_wrapper(&read_notify, NULL);
    pthread_cond_init_wrapper(&write_notify, NULL);

    uncommitted_write_count = uncommitted_read_count = 0;
}



/**
 *  @brief Mark the page buffer as closed by the consumer. If there is
 *  a producer waiting (blocked), wake it up.
 */
void page_buffer_t::stop_reading() {

    // * * * BEGIN CRITICAL SECTION * * *
    critical_section_t cs(&lock);

    // Don't bother updating the buffer size. The producer will be
    // prevented from adding to the buffer by the fact that we have
    // marked the buffer closed (with the done_reading flag).
    
    // mark the buffer closed
    done_reading = true;

    // wake the producer
    pthread_cond_signal_wrapper(&write_notify);
    
    // * * * END CRITICAL SECTION * * *
    cs.exit();
}



/**
 *  @brief Mark the page buffer as closed by the producer. If there is
 *  a consumer waiting (blocked), wake it up.
 */
void page_buffer_t::stop_writing() {

    // * * * BEGIN CRITICAL SECTION * * *
    critical_section_t cs(&lock);
    
    // Do a final update of the buffer size so the consumer does not
    // miss any of our recent additions.
    size += uncommitted_write_count;
    uncommitted_write_count = 0;

    // Mark the buffer so the consumer knows to stop once it has done
    // a final drain.
    done_writing = true;

    // notify the consumer
    pthread_cond_signal_wrapper(&read_notify);
        
    // * * * END CRITICAL SECTION * * *
    cs.exit();
}



/**
 *  @brief Read a page from the buffer. If we are down to one page in
 *  the buffer, wait for either the producer to finish or for at least
 *  threshold pages to appear in the buffer.
 *
 *  @return NULL if the buffer is empty and the producer has closed
 *  its end. Otherwise, the next page of buffer data. The caller of
 *  this function now becomes the owner of this page.
 */
page_t *page_buffer_t::read() {

    // Treat the one-page-left case separately. We avoid the
    // head==tail corner case by never allowing the buffer to empty
    // completely until the producer is finished)
    if(read_size_guess == 1) {

        // * * * BEGIN CRITICAL SECTION * * *
        critical_section_t cs(&lock);

        // We are probably going to wait for the buffer to fill
        // up. Before we deschedule ourselves, update the buffer size.
        size -= uncommitted_read_count;
        uncommitted_read_count = 0;

            
        // We want to avoid a thrashing scenario where the producer
        // inserts one page and immediately yields to us, we remove
        // the page and immediately yield to him... As long as the
        // producer is still writing, we might as well wait for it to
        // produce enough to meet our threshold.
        while(size < threshold && !done_writing) {

	    // Tell the producer to write more... why? Why would the
	    // producer be waiting if there is still plenty of space
	    // in the buffer?
            pthread_cond_signal_wrapper(&write_notify);

	    // wait for the producer to write more
            pthread_cond_wait_wrapper(&read_notify, &lock);
	}


        // We know there are at least size things in the buffer (there
        // are actually size + uncommitted_write_count)
        read_size_guess = size;
        
        // * * * END CRITICAL SECTION * * *
        cs.exit();
    }


    // empty (and therefore also done writing)?
    if(read_size_guess == 0) 
        return NULL;
    
    
    // proceed -- known not empty
    page_t *page = (page_t *)head;
    head = head->next;


    // update our counts
    read_size_guess--;
    uncommitted_read_count++;

    
    // occasionally signal the producer to let both threads work in
    // parallel
    if(uncommitted_read_count == threshold) {

        // * * * BEGIN CRITICAL SECTION * * *
        critical_section_t cs(&lock);
        
        // update the size and notify the producer
        size -= uncommitted_read_count;
        uncommitted_read_count = 0;
        pthread_cond_signal_wrapper(&write_notify);
        
        // * * * END CRITICAL SECTION * * *
        cs.exit();
    }
    
    return page;
}



/**
 *  @brief Write a page to the buffer. If the buffer is full (if there
 *  are capacity pages), wait until the free space (capacity - size)
 *  falls below threshold. If the consumer has closed his end of the
 *  buffer, return immediately without inserting.
 *
 *  @param page The page to write. We actually insert the specified
 *  page into the buffer, so the caller must give up its ownership of
 *  it.
 *
 *  @return true if the consumer has closed his end of the
 *  buffer. Future writes to the buffer will fail, returning
 *  true. false otherwise.
 */
bool page_buffer_t::write(page_t *page) {
    
    assert( page != NULL );
    
    
    // possibly full? (no corner case to worry about here)
    if(write_size_guess == capacity) {

        // * * * BEGIN CRITICAL SECTION * * *
        critical_section_t cs(&lock);

        // update the size and notify the consumer
        size += uncommitted_write_count;
        uncommitted_write_count = 0;
                
        // Want to avoid the same thrashing scenario with read(). This
        // time, wait for amount of free space in buffer to drop below
        // threshold.	
        while(size > capacity - threshold && !done_reading) {

	    // Tell the consumer to read more data from the
	    // buffer... why? Why would the consumer be waiting if
	    // there is still plenty of data in the buffer?
            pthread_cond_signal_wrapper(&read_notify);

	    // wait for the consumer to read more
            pthread_cond_wait_wrapper(&write_notify, &lock);
        }

        
	// update the guess and the size
        write_size_guess = size;

        // * * * END CRITICAL SECTION * * *
        cs.exit();
    }


    // If no one is draining the buffer, stop filling it.
    if(done_reading)
        return true;
    

    // proceed -- known not full. Corner case: very first write is to
    // an empty buffer, meaning the head and tail pointers are both
    // NULL. It's unsafe to test for this with the value of 'head'.
    if(write_size_guess == 0)
        head = page;
    else
        tail->next = page;
    tail = page;


    // update counts
    write_size_guess++;
    uncommitted_write_count++;


    // occasionally signal the consumer to encourage both threads to
    // work in parallel
    if(uncommitted_write_count == threshold) {
        // * * * BEGIN CRITICAL SECTION * * *
        critical_section_t cs(&lock);

        // update the count and notify the consumer
        size += uncommitted_write_count;
        uncommitted_write_count = 0;
        pthread_cond_signal_wrapper(&read_notify);
            
        // * * * END CRITICAL SECTION * * *
        cs.exit();
    }
    

    return false;
}



/**
 *  @brief Page buffer destructor. Current invokes free() on all
 *  pages.
 */
page_buffer_t::~page_buffer_t() {
    // free all remaining (unclaimed) pages
    while(head) {
        page_t *page = head;
        head = head->next;
        free(page);
    }
}



#include "namespace.h"
