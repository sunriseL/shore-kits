/* -*- mode:C++ c-basic-offset:4 -*- */
#ifndef _page_h
#define _page_h

#include "thread.h"
#include <new>



// include me last!!!
#include "namespace.h"



/**
 *  @brief Wapper class for a page header that stores the page's
 *  size. The constructor is private to prevent stray headers from
 *  being created in the code. We instead export a static alloc()
 *  function that allocates a new page and places a header at the
 *  base.
 */
class page_t {

 private:
    size_t _page_size;
    
 public:
    page_t *next;

 public:

    size_t page_size() const {
        return _page_size;
    }
    
    template <class Alloc>


    /**
     *  @brief The only way to create page_t instances. Allocates
     *  a new page and places the page_t as its header.
     *
     *  @param allocate Allocator for "raw" pages.
     *
     *  @param page_size The size of the new page (including
     *  page_t header).
     *
     *  @return NULL on error (if the underlying allocator returns
     *  NULL). An initialized page otherwise.
     */
    static page_t *alloc(Alloc allocate, size_t page_size=4096) {

        char *raw_page = (char *) allocate(page_size);
	if (raw_page == NULL)
	    return NULL;

        return new (raw_page) page_t(page_size);
    }

 protected:
    // called by subclass in-place constructors. No touchie fields!
    page_t() { }

 private:

    page_t(size_t page_size)
        : _page_size(page_size), next(NULL)
	{
	}

    // Prevent attempts to copy pages. The class MUST be constructed
    // in place so we can use it to refer to the page that contains
    // it. A naked set of header fields would probably result in us
    // accessing invalid memory.
    page_t(const page_t &other);
    page_t &operator=(const page_t &other);
};



/**
 *  @brief Thread-safe page buffer. This class allows one thread to
 *  safely pass pages to another. The producing thread gives up
 *  ownership of the pages it sends to the buffer.
 */
class page_buffer_t {

private:

    // number of pages allowed in the buffer
    int capacity;

    // tuning parameter
    int threshold;

    // synch vars
    pthread_mutex_t lock;
    pthread_cond_t read_notify;
    pthread_cond_t write_notify;

    // The current committed size of the buffer. The reader and writer
    // communicate through this variable, but it isn't always up to
    // date. In order to avoid serializing unnecessarily, the reader
    // and writer do not update this regularly. The true size of the
    // buffer at any given moment is 'size + uncommitted_write_count -
    // uncommitted_read_count'.
    volatile int size;

    // flags used to close the buffer
    volatile bool done_reading;
    volatile bool done_writing;

    // the singly-linked page list this buffer manages
    page_t *head;
    page_t *tail;

    // Conservative size estimates maintained by the reader and
    // writer, respectively. They are not necessarily accurate, but
    // will never cause over/underflow.
    int read_size_guess;
    int write_size_guess;

    // the number of reads/writes performed since last updating 'size'.
    int uncommitted_read_count;
    int uncommitted_write_count;
    
public:

    page_buffer_t(int capacity, int threshold=-1) {
        init(capacity, threshold);
    }
    
    ~page_buffer_t();

    bool empty() {
	// Since the page may continually be filled by its producer,
	// we can be sure that the buffer is empty if and only if the
	// producer has finished and the reader has completely drained
	// the buffer contents.
        return stopped_writing() && read_size_guess == 0;
    }
    page_t *read();
    bool write(page_t *page);

    void stop_reading();
    void stop_writing();

    bool stopped_reading() { return done_reading; }
    bool stopped_writing() { return done_writing; }
    
private:
    void init(int capacity, int threshold);
};



#include "namespace.h"
#endif
