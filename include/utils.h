/* -*- mode:C++; c-basic-offset:4 -*- */

#ifndef __UTILS_H
#define __UTILS_H

#include <cstdio>
#include <cassert>
#include "tuple.h"

/* exported datatypes */

using qpipe::tuple_buffer_t;

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


/**
 *  @brief A helper class that automatically deletes a pointer when
 *  'this' goes out of scope.
 */

template <typename T>
struct pointer_guard_t {
    T *ptr;
    pointer_guard_t(T *p)
        : ptr(p)
    {
    }

    ~pointer_guard_t() {
        if(ptr)
            delete ptr;
    }
    operator T*() {
        return ptr;
    }
    
    T *operator ->() {
        return ptr;
    }
};



/**
 *  @brief A helper class that automatically deletes an array when
 *  'this' goes out of scope.
 */

template <typename T>
struct array_guard_t {
    T *ptr;
    array_guard_t(T *p)
        : ptr(p)
    {
    }

    ~array_guard_t() {
        if(ptr)
            delete [] ptr;
    }
    operator T*() {
        return ptr;
    }
    
    T *operator ->() {
        return ptr;
    }
};



/**
 *  @brief A helper class for operations involving setup and takedown
 *  phases that must be matched (ie mutex lock/unlock, malloc/free,
 *  etc).
 *
 *  This class performs "initialization" when constructed, and asserts
 *  that "cleanup" has been performed before it goes out of scope
 *  (usually by returning from a function call). Cleanup must be
 *  performed exactly once by calling one of the "exit" functions.
 *
 *  The "State" template must provide:
 *  - typedef called "Param" that specifies the state to be protected
 *  - constructor that accepts a "Param"
 *  - init() method that performs initialization
 *  - cleanup() method that performs takedown
 */

template<class State>
class init_cleanup_t {
public:
    init_cleanup_t(const typename State::Param &param) 
        : _state(param)
    {
        init();
    }
    
    init_cleanup_t(const State &state)
        : _state(state)
    {
        init();
    }

    template <class T>
    T &exit(T &value) {
        exit();
        return value;
    }

    template <class T>
    const T &exit(const T &value) {
        exit();
        return value;
    }

    void exit() {
        assert(_active);
        _active = false;
        _state.cleanup();
    }
    
    ~init_cleanup_t() {
        assert(!_active);
    }
    
private:
    // prevent copying
    init_cleanup_t(const init_cleanup_t &);
    init_cleanup_t &operator=(const init_cleanup_t &);
    
    void init() {
        _state.init();
        _active = true;
    }
    State _state;
    bool _active;
};

#endif
