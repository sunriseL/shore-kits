
#ifndef _STAGE_FACTORY_H
#define _STAGE_FACTORY_H



#include "packet.h"
#include "stage.h"



// include me last!!!
#include "namespace.h"



/* exported datatypes */

struct stage_factory_t {

  virtual stage_t* create_stage(packet_list_t* packets, stage_container_t* sc)=0;
  virtual ~stage_factory_t() { }

};



/**
 *  @brief If a stage does not need any additional constructor
 *  parameters, we actually write a lot of uncessary code creating its
 *  factory. We create this stage_factory (NOTE NO "_t"!) to provide a
 *  quick and dirty template workaround. For example, we could type:
 *
 *  dispatcher_t::register_stage(new stage_factory<sort_stage_t>);
 *
 *  The template specifies which stage to construct.
 */
template<class STAGE>
struct stage_factory : public stage_factory_t {

  virtual stage_t* create_stage(packet_list_t* packets, stage_container_t* sc) {
    return new STAGE(packets, sc);
  }

  virtual ~stage_factory() { }
};



#include "namespace.h"



#endif
