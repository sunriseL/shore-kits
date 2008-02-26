/* -*- mode:C++; c-basic-offset:4 -*- */

/** @file shore_tpcc_order_status_baseline.h
 *
 *  @brief Declaration of the driver that submits SHORE_ORDER_STATUS_BASELINE 
 *  transaction requests, according to the TPCC specification.
 *
 *  @author Ippokratis Pandis (ipandis)
 */

#ifndef __SHORE_TPCC_ORDER_STATUS_BASELINE_DRIVER_H
#define __SHORE_TPCC_ORDER_STATUS_BASELINE_DRIVER_H


#include "workload/driver.h"
#include "stages/tpcc/common/tpcc_trx_input.h"
#include "stages/tpcc/shore/shore_order_status_baseline.h"


using namespace qpipe;
using namespace tpcc;


ENTER_NAMESPACE(workload);


class shore_tpcc_order_status_baseline_driver : public driver_t {

public:

    shore_tpcc_order_status_baseline_driver(const c_str& description)
        : driver_t(description)
    {
    }

    ~shore_tpcc_order_status_baseline_driver() { 
    }
    
    virtual void submit(void* disp, memObject_t* mem);

    trx_packet_t* create_shore_order_status_baseline_packet(tuple_fifo* bp_buffer,
                                                            tuple_filter_t* bp_filter,
                                                            int sf);
};


EXIT_NAMESPACE(workload);

#endif