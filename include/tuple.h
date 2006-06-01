/* -*- mode:C++; c-basic-offset:4 -*- */

#ifndef _TUPLE_H
#define _TUPLE_H


// must always be the first include
#include "thread.h"
#include <cassert>
#include <cstdio>

#include "page.h"


// for Dbt class
#include <db_cxx.h>



// include me last!!!
#include "namespace.h"



// exported constants

#define DEFAULT_BUFFER_PAGES 100



// exported datatypes


/**
 *  @brief QPIPE tuple. An initialized tuple stores a char* into some
 *  tuple_page_t as well as the size of the data.
 *
 * "IMPORTANT: tuple objects do *NOT* own the data array they point
 * to; pages do.  They are only convenient wrappers. Code must not
 * assume that tuples passed between functions -- or the data they
 * contain -- will remain unchanged over time. The general contract is
 * that any code that wishes to retain a tuple passed between
 * functions must make a deep copy to a page it owns, or obtain
 * ownership of the page the tuple belongs to.
 */

class tuple_t {

public:
    char* data;
    size_t size;;


    tuple_t() {
	// no data
        init(NULL, 0);
    }

    // construct a tuple from a BerkeleyDB tuple (a Dbt object)
    tuple_t(Dbt &data) {
        init((char *)data.get_data(), data.get_size());
    }
    
    tuple_t(char *data, size_t size) {
        init(data, size);
    }


    /**
     *  @brief Copy the specified tuple. This function performs a deep
     *  copy (copy of data bytes) from the src tuple to this
     *  tuple. The two tuples must already be initialized with the
     *  same lengths. The behavior of this function is undefined if
     *  this tuple does not point into a tuple page with at least
     *  src.size bytes available.
     *
     *  @param src The tuple we are creating a copy of.
     */

    void assign(const tuple_t &src) {
        assert(size == src.size);
        memcpy(data, src.data, size);
    }

private:

    /**
     *  @brief Initialize data and size.
     *
     *  @param d this.data will be set to this value.
     *
     *  @param s this.size will be set to this value.
     */

    void init(char *d, size_t s) {
        data = d;
        size = s;
    }
};



/**
 *  @brief Represents a page of tuples of the same type.
 */
class tuple_page_t : public page_t {

private:
    size_t _tuple_size;
    size_t _tuple_count;
    size_t _end_offset;
    char   _data[0];

public:

    template<class Alloc>


    /**
     *  @brief Allocate a new page and place a tuple_page_t as its
     *  header.
     *
     *  @param tuple_size The size of the tuples this page will be
     *  storing.
     *
     *  @param allocate Allocator for "raw" pages.
     *
     *  @param page_size The size of the new page (including
     *  tuple_page_t header).
     *
     *  @return NULL on error (if the underlying allocator returns
     *  NULL). An initialized page otherwise.
     */

    static tuple_page_t *alloc(size_t tuple_size,
			       Alloc allocate,
			       size_t page_size=4096) {

	page_t *page = page_t::alloc(allocate, page_size);
	if (page == NULL)
	    return NULL;

        return tuple_page_t::init(page, tuple_size);
    }


    /**
     *  @brief "Cast" the specified page with page_t header into a
     *  page with a tuple_page_t header. This function assumes that
     *  the specified page contains an initialized tuple_page_t
     *  header. It does not touch the contents of page.
     *
     *  @param page The page we are trying to mount.
     *
     *  @return An initialized page otherwise.
     */

    static tuple_page_t *mount(page_t *page) {
	
	// error checking
	assert (page != NULL);
        return new (page) tuple_page_t();
    }
    

    /**
     *  @brief Initialize the specified page with a tuple_page_t
     *  header. The tuple_page_t header will indicate that this page
     *  stores tuples of size tuple_size. It will also indicate that
     *  this page currently has no stored tuples.
     *
     *  @param page The page we are trying to initialize.
     *
     *  @param tuple_size The size of the tuples this page will be
     *  storing.
     *
     *  @return An initialized page otherwise.
     */

    static tuple_page_t *init(page_t *page, size_t tuple_size) {

	// error checking
	assert (page != NULL);
        return new (page) tuple_page_t(tuple_size);
    }


    /**
     *  @brief Returns the size of the tuples stored in this page.
     *
     *  @return See description.
     */

    size_t tuple_size()  const { return _tuple_size; }


    /**
     *  @brief Returns the number of tuples stored in this page.
     *
     *  @return See description.
     */

    size_t tuple_count() const { return _tuple_count; }


    /**
     *  @brief Returns the total number of tuples that fit into this
     *  page.
     *
     *  @return See description.
     */

    size_t capacity() const {
        return (page_size() - sizeof(tuple_page_t))/tuple_size();
    }


    /**
     *  @brief Empty this page of its tuples.
     */
    void clear() {
        _tuple_count = 0;
        _end_offset = 0;
    }
    

    /**
     *  @brief Returns true if and only if this page currently
     *  contains zero tuples.
     */

    bool empty() const {
        return !tuple_count();
    }


    /**
     *  @brief Returns true if and only if this page currently
     *  contains the maximum number of tuples it can fit.
     */

    bool full() const {
        return tuple_count() == capacity();
    }

    
    /**
     *  @brief Get the data of the tuple stored at the specified
     *  index.
     *
     *  @param index The tuple index. Tuples are zero-indexed.
     */

    char *get_tuple_data(size_t index) {
        return &_data[index*tuple_size()];
    }
    
    
    /**
     *  @brief Get a tuple_t to the tuple stored at the specified
     *  index.
     *
     *  @param index The tuple index. Tuples are zero-indexed.
     */

    tuple_t get_tuple(size_t index) {
        return tuple_t(get_tuple_data(index), tuple_size());
    }


    /**
     *  @brief Fill this page with tuples read from the specified
     *  file. If this page already contains tuples, we will only
     *  modify the remainder of the page.
     *
     *  @param file The file to read from.
     *
     *  @return The number of tuples read.
     */

    int fread(FILE *file) {

        char *base = &_data[_end_offset];
	
	// If this page is not empty, we want to leave its tuples
	// untouched. We will fill the remainder of the page.
        size_t max = capacity() - tuple_count();

        size_t count = ::fread(base, tuple_size(), max, file);

        _end_offset += count * tuple_size();
        _tuple_count += count;

	return count;
    }


    /**
     *  @brief Drain this page to the specified file.
     *
     *  @param file The file to write to.
     *
     *  @return The number of tuples written.
     */
    
    int fwrite(FILE *file) {
        return ::fwrite(_data, tuple_size(), tuple_count(), file);
    }

    
    /**
     *  @brief Allocate a new tuple on this page and initializes
     *  'tuple' to point to it. This is a pre-assembly strategy; use
     *  it to obtain tuples in order to assemble data "in place"
     *  without extra memory copies.
     *
     *  @param tuple On success, this tuple will be set to point to
     *  the newly allocated space.
     *
     *  @return true on successful allocate. false on failure (if the
     *  page is full).
     */

    bool append_mount(tuple_t &tuple) {
        tuple.size = tuple_size();
        tuple.data = allocate_tuple();
        return (tuple.data != NULL);
    }
    

    /**
     *  @brief Allocate a new tuple on this page and copy the contents
     *  of 'tuple' into it. This is a post-assembly strategy. Use it
     *  to efficiently copy existing tuples.
     *
     *  @param tuple On success, the tuple copied to this page. The
     *  size of 'tuple' must match the tuple page's internal tuple
     *  size.
     *
     *  @return true on successful allocate and copy. False if the
     *  page is * full
     */

    bool append_init(const tuple_t &tuple) {

	// error checking
	assert(tuple.size == tuple_size());

        char *data = allocate_tuple();
        if(!data)
	    // page is full!
            return false;

        // use tuple::assign() instead of naked memcpy()
        tuple_t(data, tuple_size()).assign(tuple);
        return true;
    }


    /**
     *  @brief Iterator over the tuples in this page. Each dereference
     *  returns a tuple_t.
     */

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

    /**
     *  @brief Try to allocate space for a new tuple.
     *
     *  @return NULL if the page is full. Otherwise, the address of
     *  the newly allocated tuple.
     */

    char *allocate_tuple() {

        if(tuple_count() == capacity())
	    // page is full!
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
    
    
    /**
     *  @brief Construct an empty tuple_page_t that stores tuples of
     *  the specified size.
     *
     *  @param tuple_size The size of the tuples stored in this page.
     */

    tuple_page_t(size_t tuple_size)
        : _tuple_size(tuple_size),
          _tuple_count(0),
          _end_offset(0)
    {
    }
    
};



/**
 *  @brief Thread-safe tuple buffer. This class allows one thread to
 *  safely pass tuples to another. The producer will fill a page of
 *  tuples before handing it to the consumer.
 *
 *  This implementation currently uses an internal allocator to
 *  allocate a new page every time the current page is filled and
 *  handed to the consumer.
 */
class tuple_buffer_t {

private:

    page_buffer_t page_buffer;
    size_t page_size;
    
    pthread_mutex_t init_lock;
    pthread_cond_t  init_notify;
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


    /**
     *  @brief Insert a tuple into this buffer. If the buffer is full
     *  (if it already has allocated the maximum number of pages),
     *  wait for the consumer to read. If the current page is filled
     *  and the buffer is not full, allocate a new page.
     *
     *  @param tuple The tuple to insert. On successful insert, the
     *  data that this tuple points to will be copied into this page.
     *
     *  @return 0 on successful insert. Non-zero if the buffer was
     *  closed by the consumer.
     */

    int put_tuple (const tuple_t &tuple) {

        if(check_page_full())
            return 1;

        return !write_page->append_init(tuple);
    }


    /**
     *  @brief Insert a page of tuples to the buffer in one
     *  operation. The buffer assumes ownership of the page and is now
     *  responsible for destroying it. This function will block if the
     *  tuple_buffer_t already contains the maximum number of pages.
     *
     *  WARNING: Do not mix calls to put_page() and put_tuple() if
     *  tuple ordering is important (if tuples must be consumed in the
     *  same order in which they were inserted).
     *
     *  @param page The page of tuples to insert.
     * 
     *  @return 0 on successful insert. Non-zero if the buffer was
     *  closed by the consumer.
     */

    int put_page(tuple_page_t *page) {
        return page_buffer.write(page);
    }

    
    /**
     *  @brief Allocate space for a new tuple in this buffer and set
     *  'tuple' to point to it. The tuple buffer specifies the size to
     *  allocate. This function blocks if the buffer is full.
     *
     *  If this function returns success, the caller may invoke
     *  tuple.assign() to copy data into the allocated space. This
     *  should be done before the next put_tuple() or alloc_tuple()
     *  operation to prevent uninitialized data from being fed to the
     *  consumer of this buffer.
     *
     *  @return 0 on successful allocate. Non-zero if the consumer has
     *  closed the buffer or if the current page of the buffer is
     *  full.
     */

    int alloc_tuple(tuple_t &tuple) {
        if(check_page_full())
            return 1;

        return !write_page->append_mount(tuple);
    };
  
    
    bool get_tuple(tuple_t &tuple);


    /**
     *  @brief Retrieve a page of tuples from the buffer in one
     *  operation. The buffer gives up ownership of the page and will
     *  not access (or free) it afterward.
     *
     *  WARNING: Do not mix calls to get_page() and get_tuple() unless
     *  the caller if prepared to deal with duplicate tuples. In other
     *  words, the caller must be willing to process some unknown
     *  number of tuples twice.
     *
     *  @return NULL on error (if the buffer is empty and the producer
     *  has closed the buffer). Otherwise, a page of tuples (some of
     *  which may have been returned previously by calls to
     *  get_tuple()).
     */

    tuple_page_t *get_page() {

	// cast the next page in the buffer to a page with a
	// tuple_page_t header
        return tuple_page_t::mount(page_buffer.read());
    }


    int wait_for_input(); 
    void close();
    void send_eof();


    /**
     *  @brief Check if we have consumed all tuples from this
     *  buffer. In other words, returns true if and only if the
     *  producer has closed its end of the buffer and we have read
     *  every tuple of every page in this buffer.
     */

    bool eof() {

        return page_buffer.stopped_writing()
            && page_buffer.empty()
	    
	    // The final read_page remains allocated until
	    // tuple_buffer_t is destroyed
            && read_iterator == read_page->end();
    }
 

    void init_buffer();
    int wait_init(bool block=true);


    /**
     *  @brief Send a message to the monitor only if the change is
     *  over 10% of the last transmitted change. The last transmitted
     *  value is stored in the lastTransmit variable. This function is
     *  currently not implemented.
     */

    void send_update();
  

    /**
     *  @brief Construct a tuple buffer that holds tuples of the
     *  specified size.
     *
     *  @param tuple_size The size of the tuples stored in this
     *  tuple_buffer_t.
     *
     *  @param num_pages We would like to allocate a new page whenever
     *  we fill up the current page. If the buffer currently contains
     *  this many pages, our insert operations will block rather than
     *  allocate new pages.
     *
     *  @param page_size The size of the pages used in our buffer.
     */

    tuple_buffer_t(size_t tuple_size,
                   int num_pages=DEFAULT_BUFFER_PAGES,
                   int page_size=4096)
        : page_buffer(num_pages)
    {
        init(tuple_size, page_size);
    }


    ~tuple_buffer_t();


protected:

    int check_page_full();

    int init(size_t tuple_size, size_t num_pages);
};



#include "namespace.h"
#endif
