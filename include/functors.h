/* -*- mode:C++; c-basic-offset:4 -*- */

#ifndef __FUNCTORS_H
#define __FUNCTORS_H

#include "tuple.h"

#include "engine/namespace.h"



/** 
 * @brief Base selection/projection functor. Should be extended to
 * implement other selection/projection functors.
 */

class tuple_filter_t {
private:
    size_t _tuple_size;
    
public:

    size_t input_tuple_size() { return _tuple_size; }
    
    /**
     *  @brief Determine which attributes (if any) of a given source
     *  tuple pass an internal set of predicates. Should be applied to
     *  the output of a stage to implement either selection or
     *  projection.
     *
     *  This default implementation allows every attribute of every
     *  source tuple to pass.
     *
     *  @param src The tuple we are checking.
     *
     *  @return True if the specified tuple passes the
     *  selection. False otherwise.
     */

    virtual bool select(const tuple_t &) {
        return true;
    }


    /**
     *  @brief Project some threads from the src to the dest tuple.
     *
     *  This default implementation copies the entire tuple, so dest
     *  must point to a location with at least src.size bytes
     *  available.
     *
     *  @param src Our source tuple.
     *
     *  @param dest We copy the necessary attributes here. This tuple
     *  should already point into a tuple_buffer_t where we can store
     *  data.
     *
     *  @return void
     */

    virtual void project(tuple_t &dest, const tuple_t &src) {
        dest.assign(src);
    }


    tuple_filter_t(size_t input_tuple_size)
        : _tuple_size(input_tuple_size)
    {
    }
    
    virtual ~tuple_filter_t() { }

};



/** 
 * @brief Base join functor. Should be used by join stages to
 * determine if two source tuples pass the join predicate. Should be
 * extended to implement other join functors.
 */

class tuple_join_t {

public:


    /**
     *  @brief Determine whether two tuples pass an internal set of join
     *  predicates. Should be applied within a join stage.
     *
     *  @param dest If the two tuples pass the specified join
     *  predicates, a composite of the two is assigned here.
     *
     *  @param A tuple from the outer relation.
     *
     *  @param A tuple from the inner relation.
     *
     *  @return True if the specified tuples pass the join
     *  predicates. False otherwise.
     */

    virtual bool join(tuple_t &dest,
                      const tuple_t &left,
                      const tuple_t &right)=0;
    
    virtual ~tuple_join_t() { }
};



/** 
 * @brief Base aggregation functor. Should be extended to implement
 * other aggregation functors.
 */
class tuple_aggregate_t {

public:

    
    /**
     *  @brief Update the internal aggregate state and determine if a
     *  new output tuple can be produced (if a group break occurred).
     *
     *  @param dest If an output tuple is produced, it is assigned to
     *  this tuple.
     *
     *  @return True if an output tuple is produced. False otherwise.
     */

    virtual bool aggregate(tuple_t &dest, const tuple_t &src)=0;


    /**
     *  @brief Do a final update of the internal aggregate state and
     *  (possibly) produce a final output tuple. This method should be
     *  invoked when there are no more source tuples to be aggregated.
     *
     *  @param dest If an output tuple is produced, it is assigned to
     *  this tuple.
     *
     *  @return True if an output tuple is produced. False otherwise.
     */

    virtual bool eof(tuple_t &dest)=0;
    
  
    virtual ~tuple_aggregate_t() { }

};


/**
 * @brief A key-tuple pair used for operations such as hashing and
 * sorting. The first 32 bits of the key (or hash) are stored directly
 * and should suffice for most comparisons; the full key can always be
 * accessed by following the pointer to the tuple's data.
 */

struct key_tuple_pair_t {
    int key;
    char *data;

    key_tuple_pair_t()
        : key(0), data(NULL)
    {
    }
    key_tuple_pair_t(int k, char *d)
        : key(k), data(d)
    {
    }
};



/**
 *@brief Base comparison functor. Extend it to implement specific
 *comparisons on tuple field(s). 
 */

struct tuple_comparator_t {
    
    /**
     * @brief Determines the lexicographical relationship between
     * tuples (a) and (b). Ascending sorts based on this
     * comparison will produce outputs ordered such that
     * compare(tuple[i], tuple[i+1]) < 0 for every i.
     *
     * @return negative if a < b, positive if a > b, and zero if a == b
     */
    
    virtual int compare(const key_tuple_pair_t &a, const key_tuple_pair_t &b)=0;



    /**
     * @brief returns the 32-bit key this comparator uses for primary
     * comparisons. See (key_tuple_pair_t).
     */
    
    virtual int make_key(const tuple_t &tuple)=0;


    
    virtual ~tuple_comparator_t() { }
};



#include "engine/namespace.h"



#endif	// __FUNCTORS_H
