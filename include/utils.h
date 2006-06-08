/* -*- mode:C++; c-basic-offset:4 -*- */

#ifndef __UTILS_H
#define __UTILS_H

#include <cstdio>
#include <cassert>
#include "trace/trace.h"


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
    pointer_guard_t(T *ptr=NULL)
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
private:
    pointer_guard_t &operator=(pointer_guard_t &);
};

/**
 * @brief Ensures that an array allocated with a call to operator
 * new() gets deleted
 */
template <class T>
struct array_guard_t : public pointer_guard_base_t<T, array_guard_t<T> > {
    array_guard_t(T *ptr=NULL)
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
private:
    array_guard_t &operator=(array_guard_t &);
};

/**
 * @brief Ensures that a file gets closed
 */
struct file_guard_t : public pointer_guard_base_t<FILE, file_guard_t> {
    file_guard_t(FILE *ptr=NULL)
        : pointer_guard_base_t<FILE, file_guard_t>(ptr)
    {
    }
    struct Action {
        void operator()(FILE *ptr) {
            if(fclose(ptr)) 
                TRACE(TRACE_ALWAYS, "fclose failed");
        }
    };
private:
    file_guard_t &operator=(file_guard_t &);
};

#endif
