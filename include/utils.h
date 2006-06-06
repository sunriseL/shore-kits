/* -*- mode:C++; c-basic-offset:4 -*- */

#ifndef __UTILS_H
#define __UTILS_H

#include <cstdio>
#include <cassert>
#include "tuple.h"

/* exported datatypes */

using qpipe::tuple_buffer_t;
using qpipe::page_t;
using qpipe::tuple_page_t;

/**
 * @brief A generic pointer guard base class. An action will be
 * performed exactly once on the stored pointer, either with an
 * explicit call to done() or when the guard goes out of scope,
 * whichever comes first.
 *
 * This is very similar to the auto_ptr class except:
 * - the "auto" part is configurable.
 * - it is unknown whether passing to it functions by value is safe
 */
template <class T, class Child>
class pointer_guard_base_t {
    T *_ptr;

public:
    typedef pointer_guard_base_t<T, Child> Base;
    struct temp_ref_t {
        T *_ptr;
        temp_ref_t(T *ptr)
            : _ptr(ptr)
        {
        }
    };
    
public:
    pointer_guard_base_t(T *ptr)
        : _ptr(ptr)
    {
    }
    pointer_guard_base_t(const temp_ref_t &other)
        : _ptr(other._ptr)
    {
    }

    pointer_guard_base_t &operator=(const temp_ref_t &other) {
        assign(other._ptr);
        return *this;
    }

    T &operator *() {
        return *_ptr;
    }
    operator T*() {
        return _ptr;
    }
    T *operator ->() {
        return _ptr;
    }
    
    /**
     * @brief Notifies this guard that its action should be performed
     * now rather than at destruct time.
     */
    void done() {
        if(_ptr) {
            typename Child::Action()(_ptr);
            _ptr = NULL;
        }
    }
    
    /**
     * @brief Notifies this guard that its services are no longer
     * needed because some other entity has assumed ownership of the
     * pointer. 
     */
    T *release() {
        T *ptr = _ptr;
        _ptr = NULL;
        return ptr;
    }

    void assign(T *ptr) {
        done();
        _ptr = ptr;
    }
    
    ~pointer_guard_base_t() {
        done();
    }
};

/**
 * @brief Ensures that a pointer allocated with a call to operator
 * new() gets deleted
 */
template <class T>
struct pointer_guard_t : public pointer_guard_base_t<T, pointer_guard_t<T> > {
    pointer_guard_t(T *ptr)
        : pointer_guard_base_t<T, pointer_guard_t<T> >(ptr)
    {
    }
    
    struct Action {
        void operator()(T *ptr) {
            delete ptr;
        }
    };

    pointer_guard_t &operator=(const typename pointer_guard_t::temp_ref_t &ref) {
        assign(ref._ptr);
        return *this;
    }
};

/**
 * @brief Ensures that an array allocated with a call to operator
 * new() gets deleted
 */
template <class T>
struct array_guard_t : public pointer_guard_base_t<T, array_guard_t<T> > {
    array_guard_t(T *ptr)
        : pointer_guard_base_t<T, array_guard_t<T> >(ptr)
    {
    }
    
    struct Action {
        void operator()(T *ptr) {
            delete [] ptr;
        }
    };

    array_guard_t &operator=(const typename array_guard_t::temp_ref_t &ref) {
        assign(ref._ptr);
        return *this;
    }
};

/**
 * @brief Ensures that a page allocated with malloc+placement-new gets freed
 */
struct page_guard_t : public pointer_guard_base_t<tuple_page_t, page_guard_t> {
    page_guard_t(tuple_page_t *ptr)
        : pointer_guard_base_t<tuple_page_t, page_guard_t>(ptr)
    {
    }
    
    struct Action {
        void operator()(tuple_page_t *ptr) {
            free(ptr);
        }
    };

    page_guard_t &operator=(const page_guard_t::temp_ref_t &ref) {
        assign(ref._ptr);
        return *this;
    }
};

/**
 * @brief Ensures that a list of pages allocated with malloc+placement-new is freed 
 */
struct page_list_guard_t : public pointer_guard_base_t<page_t, page_list_guard_t> {
    page_list_guard_t(page_t *ptr=NULL)
        : pointer_guard_base_t<page_t, page_list_guard_t>(ptr)
    {
    }
    struct Action {
        void operator()(page_t *ptr) {
            while(ptr) {
                page_t *page = ptr;
                ptr = page->next;
                free(page);
            }
        }
    };
    
    void append(page_t *ptr) {
        assert(ptr);
        ptr->next = release();
        assign(ptr);
    }

    page_list_guard_t &operator=(const page_list_guard_t::temp_ref_t &ref) {
        assign(ref._ptr);
        return *this;
    }
};

/**
 * @brief Ensures that a file gets closed
 */
struct file_guard_t : public pointer_guard_base_t<FILE, file_guard_t> {
    file_guard_t(FILE *ptr)
        : pointer_guard_base_t<FILE, file_guard_t>(ptr)
    {
    }
    struct Action {
        void operator()(FILE *ptr) {
            fclose(ptr);
        }
    };
};

/**
 *  @brief Convenient wrapper around a tuple_buffer_t that will ensure
 *  that the buffer is initialized, then close it when this wrapper
 *  goes out of scope. By using this wrapper, we can avoid duplicating
 *  close() code at every exit point.
 */
struct buffer_guard_t {
    tuple_buffer_t *buffer;
    buffer_guard_t(tuple_buffer_t *buf=NULL) {
        *this = buf;
    }
  
    ~buffer_guard_t() {
        if(buffer)
            buffer->close();
    }

    buffer_guard_t &operator=(tuple_buffer_t *buf) {
        buffer = buf;
        if(buffer)
            buffer->init_buffer();
        
        return *this;
    }

    tuple_buffer_t *operator->() {
        return buffer;
    }

    operator tuple_buffer_t*() {
        return buffer;
    }
private:
    // no copying
    buffer_guard_t &operator=(const buffer_guard_t &other);
    buffer_guard_t(const buffer_guard_t &other);
};

#endif
