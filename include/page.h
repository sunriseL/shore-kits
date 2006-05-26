/* -*- mode: C++ -*- */
#ifndef _page_h
#define _page_h

#include <pthread.h>
#include <new>

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
    static page_t *alloc(Alloc allocate, size_t page_size=4096) {
        char *raw_page = (char *) allocate(page_size);
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

    // prevent attempts to copy pages because the class MUST be
    // constructed in place; it doesn't even store a pointer to page
    // data. A naked set of header fields would probably cause all
    // kinds of memory bugs
    page_t(const page_t &other);
    page_t &operator=(const page_t &other);
};


/* A thread-safe page buffer. This class allows one thread to safely
   pass pages to another. The producing thread gives up ownership of
   the pages it sends to the buffer.
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

    // the current committed size of the buffer. The reader and writer
    // communicate through this variable, but it isn't always up to
    // date. In order to avoid serializing unnecessarily the reader
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

    // conservative size estimates maintained by the reader and
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

#endif
