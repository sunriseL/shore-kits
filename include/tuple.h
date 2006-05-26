// -*- C++ -*-

#ifndef _tuple_h
#define _tuple_h

// must always be the first include
#include <pthread.h>
#include <cassert>
#include <cstdio>

#include "page.h"

// for Dbt class
#include <db_cxx.h>

// include me last!!!
#include "namespace.h"

#define DEFAULT_BUFFER_PAGES 100
/**
 * class tuple_t
 *
 * Structure that represents a tuple. A tuple consists of the data 
 * (in the form of a char*), and the size of the data.
 *
 * IMPORTANT: tuple objects do *NOT* own the data array they point to;
 * pages do.  They are only convenient wrappers. Code must not assume
 * that tuples passed between functions -- or the data they contain --
 * will remain unchanged over time. The general contract is that any
 * code that wishes to retain a tuple passed between functions must
 * make a deep copy to a page it owns, or obtain ownership of the page
 * the tuple belongs to.
 */
class tuple_t {
public:
    char* data;
    size_t size;;

    tuple_t() {
        init(NULL, 0);
    }

    // allows Dbt objects to be treated as tuples
    tuple_t(Dbt &data) {
        init((char *)data.get_data(), data.get_size());
    }
    
    tuple_t(char *data, size_t size) {
        init(data, size);
    }

    /* Copies the contents of 'tuple' into this one. The declared data
       lengths must match and this tuple must have enough space
       allocated in its 'data' array.
    */
    void assign(const tuple_t &tuple) {
        assert(size == tuple.size);
        memcpy(data, tuple.data, size);
    }

private:
    void init(char *d, size_t s) {
        data = d;
        size = s;
    }
};

class tuple_page_t : public page_t {
private:
    size_t _tuple_size;
    size_t _tuple_count;
    size_t _end_offset;
    char _data[0];

public:
    template<class Alloc>
    static tuple_page_t *alloc(size_t tuple_size,
                                  Alloc allocate,
                                  size_t page_size=4096) {
        page_t *page = page_t::alloc(allocate, page_size);
        return tuple_page_t::init(page, tuple_size);
    }
    
    static tuple_page_t *mount(page_t *page) {
        return new (page) tuple_page_t();
    }

    static tuple_page_t *init(page_t *page, size_t tuple_size) {
        return new (page) tuple_page_t(tuple_size);
    }
    
    size_t tuple_size() const { return _tuple_size; }
    
    size_t tuple_count() const { return _tuple_count; }
    
    size_t capacity() const {
        return (page_size() - sizeof(tuple_page_t))/tuple_size();
    }

    void clear() {
        _tuple_count = 0;
        _end_offset = 0;
    }
    
    bool empty() const {
        return !tuple_count();
    }

    bool full() const {
        return tuple_count() == capacity();
    }

    char *get_tuple_data(size_t index) {
        return &_data[index*tuple_size()];
    }
    
    tuple_t get_tuple(size_t index) {
        return tuple_t(get_tuple_data(index), tuple_size());
    }

    /* Fills this page with tuples read from (file).
       Returns the number of tuples read in.
     */
    int fread(FILE *file) {
        char *base = &_data[_end_offset];
        int max = capacity() - tuple_size();
        int count = ::fread(base, tuple_size(), max, file);
        _end_offset += count * tuple_size();
        _tuple_count += count;
        return count;
    }

    /* Writes the tuples on this page out to (file).
       Returns the number of tuples written out.
    */
    int fwrite(FILE *file) {
        return ::fwrite(_data, tuple_size(), tuple_count(), file);
    }
    
    /* Allocates a new tuple on this page and initializes 'tuple' to
       point to it. This is a pre-assembly strategy; use it to obtain
       tuples in order to assemble data "in place" without extra
       memory copies. Return false if the page is full.
     */
    bool append_mount(tuple_t &tuple) {
        tuple.size = tuple_size();
        tuple.data = allocate_tuple();
        return (tuple.data != NULL);
    }
    
    /* Allocates a new tuple on this page and copies the contents of
       'tuple' into it. This is a post-assembly strategy. Use it to
       efficiently clone existing tuples. Return false if the page is
       full
    */
    bool append_init(const tuple_t &tuple) {
        char *data = allocate_tuple();
        if(!data)
            return false;

        // let the tuple perform the memcpy
        tuple_t(data, tuple_size()).assign(tuple);
        return true;
    }

    // a fully-fledged iterator
    class iterator {
    private:
        tuple_t _current;
        
    public:
        iterator()
            : _current(NULL, 0)
        {
        }
        iterator(tuple_page_t *page, size_t offset)
            : _current(page->_data + offset,
                       page->tuple_size())
        {
        }
        iterator(size_t size, char *data)
            : _current(data, size)
        {
        }

        bool operator ==(const iterator &other) const {
            return _current.data == other._current.data;
        }
        bool operator !=(const iterator &other) const {
            return !(*this == other);
        }
        tuple_t &operator *() {
            return _current;
        }
        tuple_t *operator ->() {
            return &_current;
        }
        iterator operator +(int index) const {
            int offset = index*_current.size;
            return iterator(_current.size, _current.data + offset);
        }
        iterator operator -(int offset) const {
            return *this + -offset;
        }
        iterator &operator ++() {
            _current.data += _current.size;
            return *this;
        }
        iterator operator ++(int) {
            iterator old = *this;
            ++*this;
            return old;
        }
    };
    
    iterator begin() {
        return iterator(this, 0);
    }
    
    iterator end() {
        return iterator(this, _end_offset);
    }
    
protected:
    /* attempts to allocate space for a new tuple. Return NULL if the
    page is full.
    */
    char *allocate_tuple() {
        if(tuple_count() == capacity())
            return NULL;
        
        char *result = &_data[_end_offset];
        _end_offset += tuple_size();
        _tuple_count++;
        return result;
    }
    
    tuple_page_t() {
        // sanity check
        assert(tuple_size()*tuple_count() == _end_offset);
    }
    
    tuple_page_t(size_t tuple_size)
        : _tuple_size(tuple_size),
          _tuple_count(0),
          _end_offset(0)
    {
    }
    
};

class tuple_buffer_t {
private:
    page_buffer_t page_buffer;
    size_t page_size;
    
    pthread_mutex_t init_lock;
    pthread_cond_t init_notify;

    volatile bool initialized;

    int lastTransmit;
    int sample;


    tuple_page_t *read_page;
    tuple_page_t::iterator read_iterator;
    tuple_page_t *write_page;

    bool input_arrived;
public:

    int unique_id;
    size_t tuple_size;
    
    /* puts tuple in the buffer
     * blocks the calling thread if the buffer is full
     * return 0 if the insert was succesful
     * return 1 if the buffer is closed by consumer
     */
    int put_tuple (const tuple_t &tuple) {
        return !write_page->append_init(tuple);
    }

    /* Adds a page of tuples to the buffer in one operation. The
       buffer assumes ownership of the page and will free it
       appropriately.

       WARNING: it is only safe to mix calls to put_page() and
       put_tuple() if tuple ordering is unimportant.
       
       return 1 if the buffer has been closed
    */
    int put_page(tuple_page_t *page) {
        return page_buffer.write(page);
    }

    /* Allocates a new tuple in the buffer, setting 'tuple' to point
       to it. Blocks the calling thread if the buffer is full. The
       newly allocated tuple will be added to the buffer as part of
       the next call to put_tuple() or alloc_tuple()

       return 1 if the buffer has been closed
    */
    int alloc_tuple(tuple_t &tuple) {
        return !write_page->append_mount(tuple);
    };
  
    /* retrieves a tuple from buffer
     * blocks the calling thread if the buffer is empty
     * return NULL if the producer finished producing tuples
     */
    int get_tuple(tuple_t &tuple);

    /* Retrieves a page of tuples from the buffer in one
       operation. The buffer gives up ownership of the page and will
       not access (or free) it afterward.

       WARNING: it is only safe to mix calls to get_page() and
       get_tuple() if the caller is willing to process some unknown
       number of tuples twice.

       return NULL if EOF
    */
    tuple_page_t *get_page() {
        return tuple_page_t::mount(page_buffer.read());
    }

    /* Blocks until either inputs become available or the writer sends
       eof. Returns FALSE if EOF */
    int wait_for_input(); 
  
    /* closes an input_buffer
     * following invocations of put_tuple will return 1
     */
    void close_buffer();

    /* Signals the consumer that there will be no more inputs.
     */
    void send_eof();

    /* Determines if all tuples have been read */
    bool eof() {
        return page_buffer.stopped_writing()
            && page_buffer.empty()
            && read_iterator == read_page->end();
    }
  
    /* called by consumer to initiate the buffer
     */
    void init_buffer();
  
    /* called by consumer to wait until the consumer initiated the communications
     * 0 indicates a success, negative value indicates parent terminated us
     * positive value indicates that waiting_init would block and we should try calling later
     */
    int wait_init(bool block = true);

    /* send a message to the monitor only if the change is over 10% of the last
     * transmitted change. the last transmitted value is stored in the lastTransmit
     * variable
     */
    void send_update();
  
    /* buffer constructor and destructor */
    tuple_buffer_t(size_t tuple_size,
                   int num_pages=DEFAULT_BUFFER_PAGES,
                   int page_size=4096)
        : page_buffer(num_pages)
    {
        init(tuple_size, page_size);
    }
    ~tuple_buffer_t();

protected:
    // checks for a full write page and adds it to the internal page
    // buffer if necessary. Also checks for cancellation.  return 1 if
    // cancelled
    int check_page_full();
    
    void init(size_t tuple_size, size_t num_pages);
};

#include "namespace.h"
#endif
