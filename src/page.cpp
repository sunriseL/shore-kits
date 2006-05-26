#include "page.h"
#include "pthread_wrappers.h"

void page_buffer_t::init(int capacity, int threshold) {
    page_buffer_t::capacity = capacity;
    if(threshold < 0)
        page_buffer_t::threshold = (int) (.3*capacity);
    else
        page_buffer_t::threshold = threshold;
    
    size = 0;
    read_size_guess = 1;
    write_size_guess = 0;

    head = tail = NULL;
    done_reading = done_writing = false;

    pthread_mutex_init_wrapper(&lock, NULL);
    pthread_cond_init_wrapper(&read_notify, NULL);
    pthread_cond_init_wrapper(&write_notify, NULL);

    uncommitted_write_count = uncommitted_read_count = 0;
}

void page_buffer_t::stop_reading() {
    { // * * * BEGIN CRITICAL SECTION * * *
        pthread_mutex_lock_wrapper(&lock);

        // notify the writer
        done_reading = true;
        pthread_cond_signal_wrapper(&write_notify);
        
        pthread_mutex_unlock_wrapper(&lock);
    } // * * * END CRITICAL SECTION * * *
}

void page_buffer_t::stop_writing() {
    { // * * * BEGIN CRITICAL SECTION * * *
        pthread_mutex_lock_wrapper(&lock);

        // notify the reader
        size += uncommitted_write_count;
        uncommitted_write_count = 0;
        done_writing = true;
        pthread_cond_signal_wrapper(&read_notify);
        
        pthread_mutex_unlock_wrapper(&lock);
    } // * * * END CRITICAL SECTION * * *
}

page_t *page_buffer_t::read() {

    // possibly nearly empty? (avoid the head == tail corner case by
    // not ever allowing the buffer to empty completely until the
    // writer is finished)
    if(read_size_guess == 1) {
        { // * * * BEGIN CRITICAL SECTION * * *
            pthread_mutex_lock_wrapper(&lock);

            // update the size and notify the writer
            size -= uncommitted_read_count;
            uncommitted_read_count = 0;
            
            // avoid thrashing
            while(size < threshold && !done_writing) {
                pthread_cond_signal_wrapper(&write_notify);
                pthread_cond_wait_wrapper(&read_notify, &lock);
            }

            // update the guess
            read_size_guess = size;
            
            pthread_mutex_unlock_wrapper(&lock);
        } // * * * END CRITICAL SECTION * * *
    }

    // empty (and therefore also done writing)?
    if(read_size_guess == 0) 
        return NULL;
    
    // proceed -- known not empty
    page_t *page = (page_t *)head;
    head = head->next;

    // update counts
    read_size_guess--;
    uncommitted_read_count++;

    // occasionally signal the writer to let both threads work in parallel
    if(uncommitted_read_count == threshold) {
        { // * * * BEGIN CRITICAL SECTION * * *
            pthread_mutex_lock_wrapper(&lock);
            
            // update the size and notify the writer
            size -= uncommitted_read_count;
            uncommitted_read_count = 0;
            pthread_cond_signal_wrapper(&write_notify);
            
            pthread_mutex_unlock_wrapper(&lock);
        } // * * * END CRITICAL SECTION * * *
    }
    
    return page;
}

bool page_buffer_t::write(page_t *page) {
    
    // possibly full? (no corner case to worry about here)
    if(write_size_guess == capacity) {
        { // * * * BEGIN CRITICAL SECTION * * *
            pthread_mutex_lock_wrapper(&lock);

            // update the size and notify the reader
            size += uncommitted_write_count;
            uncommitted_write_count = 0;
                
            // avoid thrashing
            while(size > capacity - threshold && !done_reading) {
                pthread_cond_signal_wrapper(&read_notify);
                pthread_cond_wait_wrapper(&write_notify, &lock);
            }

            // update the guess and the size
            write_size_guess = size;

            pthread_mutex_unlock_wrapper(&lock);
        } // * * * END CRITICAL SECTION * * *
    }

    // cancelled?
    if(done_reading)
        return true;
    
    // proceed -- known not full. Corner case: very first write is to
    // an empty buffer, meaning the head and tail pointers are both
    // NULL. It's unsafe to test for  this with the value of 'head'.
    if(write_size_guess == 0)
        head = page;
    else
        tail->next = page;
    tail = page;

    // update counts
    write_size_guess++;
    uncommitted_write_count++;

    // occasionally signal the reader to encourage both threads to
    // work in parallel
    if(uncommitted_write_count == threshold) {
        { // * * * BEGIN CRITICAL SECTION * * *
            pthread_mutex_lock_wrapper(&lock);

            // update the count and notify the reader
            size += uncommitted_write_count;
            uncommitted_write_count = 0;
            pthread_cond_signal_wrapper(&read_notify);
            
            pthread_mutex_unlock_wrapper(&lock);
        } // * * * END CRITICAL SECTION * * *
    }
    
    return false;
}

page_buffer_t::~page_buffer_t() {
    // free all remaining (unclaimed) pages
    while(head) {
        page_t *page = head;
        head = head->next;
        free(page);
    }
}
