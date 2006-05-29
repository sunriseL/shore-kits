/* -*- mode: C++ -*- */

#ifndef __FUNCTORS_H
#define __FUNCTORS_H

#include "tuple.h"

#include "namespace.h"

# define FUNCTOR_ID_SIZE 256

class tuple_filter_t {
public:
    virtual bool filter(tuple_t &dest, const tuple_t &src) {
        dest.assign(src);
        return true;
    }
    virtual ~tuple_filter_t() { }
    
};

class tuple_join_t {
public:
    virtual void join(tuple_t &dest,
                      const tuple_t &left,
                      const tuple_t &right)=0;
    
    virtual ~tuple_join_t() { }
};

class tuple_aggregate_t : public tuple_filter_t {
public:
    virtual void eof(tuple_t &dest)=0;
    
    virtual ~tuple_aggregate_t() { }
};

#include "namespace.h"
#endif	// __FUNCTORS_H
