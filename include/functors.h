/* -*- mode: C++ -*- */

#ifndef __FUNCTORS_H
#define __FUNCTORS_H

#include "tuple.h"

#include "namespace.h"




/** 
 * @brief Base selection/projection functor. Should be extended to
 * implement other selection/projection functors.
 */
class tuple_filter_t {

public:

  /**
   *  @brief Determine which attributes (if any) of a given source
   *  tuple pass an internal set of predicates. Should be applied to
   *  the output of a stage to implement either selection or
   *  projection. This default implementation allows every attribute
   *  of every source tuple to pass.
   *
   *  @param dest If the src tuple passes this filter, it is assigned
   *  to this tuple.
   *
   *  @param src The tuple we are testing with the filter.
   *
   *  @return True if the specified tuple passes the filter. False
   *  otherwise.
   */
  virtual bool filter(tuple_t &dest, const tuple_t &src) {
        dest.assign(src);
        return true;
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
class tuple_aggregate_t : public tuple_filter_t {

public:

  /**
   *  @brief We would like to use the same functor to implement both
   *  scalar and GROUP-BY aggregation. The filter() function inherited
   *  from tuple_filter_t produces an output tuple (returns true)
   *  every time it detects a GROUP-BY attribute transition. This if
   *  called to produce the final output tuple when all input tuples
   *  have been aggregated.
   *
   *  @param dest If an output tuple is produced, it is assigned to
   *  this tuple.
   *
   *  @return True if an output tuple is produced. False otherwise.
   */
  virtual bool eof(tuple_t &dest)=0;
    
  
  virtual ~tuple_aggregate_t() { }

};

#include "namespace.h"
#endif	// __FUNCTORS_H
