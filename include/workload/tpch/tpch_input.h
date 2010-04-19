/* -*- mode:C++; c-basic-offset:4 -*-
   Shore-kits -- Benchmark implementations for Shore-MT
   
   Copyright (c) 2007-2009
   Data Intensive Applications and Systems Labaratory (DIAS)
   Ecole Polytechnique Federale de Lausanne
   
   All Rights Reserved.
   
   Permission to use, copy, modify and distribute this software and
   its documentation is hereby granted, provided that both the
   copyright notice and this permission notice appear in all copies of
   the software, derivative works or modified versions, and any
   portions thereof, and that both notices appear in supporting
   documentation.
   
   This code is distributed in the hope that it will be useful, but
   WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. THE AUTHORS
   DISCLAIM ANY LIABILITY OF ANY KIND FOR ANY DAMAGES WHATSOEVER
   RESULTING FROM THE USE OF THIS SOFTWARE.
*/

/* -*- mode:C++; c-basic-offset:4 -*- */

/** @file tpch_input.h
 *
 *  @brief Declaration of the (common) inputs for the TPC-H trxs
 *  @brief Declaration of functions that generate the inputs for the TPCH TRXs
 *
 *  @author Ippokratis Pandis (ipandis)
 */

#ifndef __TPCH_INPUT_H
#define __TPCH_INPUT_H

#include "workload/tpch/tpch_const.h"
#include "workload/tpch/tpch_struct.h"


ENTER_NAMESPACE(tpch);

// Inputs for the Queries


/******************************************************************** 
 *
 *  Q1
 *
 ********************************************************************/


struct q1_input_t 
{
    time_t l_shipdate;
    q1_input_t& operator=(const q1_input_t& rhs);
};

q1_input_t    create_q1_input(const double sf, 
                              const int specificWH = 0);



/******************************************************************** 
 *
 *  Q6
 *
 ********************************************************************/

struct q6_input_t 
{
    time_t l_shipdate;
    int    l_quantity;
    float  l_discount;

    q6_input_t& operator=(const q6_input_t& rhs);
};

q6_input_t    create_q6_input(const double sf, 
                              const int specificWH = 0);



/******************************************************************** 
 *
 *  Q12
 *
 ********************************************************************/

struct q12_input_t 
{
    char   l_shipmode1 [STRSIZE(10)];
    char   l_shipmode2 [STRSIZE(10)];
    time_t l_receiptdate; 

    q12_input_t& operator=(const q12_input_t& rhs);
};

q12_input_t    create_q12_input(const double sf, 
                                const int specificWH = 0);


/******************************************************************** 
 *
 *  Q14
 *
 ********************************************************************/

struct q14_input_t 
{
    time_t l_shipdate;

    q14_input_t& operator=(const q14_input_t& rhs);
};

q14_input_t    create_q14_input(const double sf, 
                                const int specificWH = 0);




/******************************************************************** 
 *
 *  Q-NP
 *
 ********************************************************************/

struct qNP_input_t 
{
    int _custid;

    qNP_input_t& operator=(const qNP_input_t& rhs);
};

qNP_input_t    create_qNP_input(const double sf, 
                                const int specificWH = 0);




/******************************************************************** 
 *
 *  Inputs for the TPC-H Database Population
 *
 ********************************************************************/

struct populate_baseline_input_t 
{
    double _sf;
    int _loader_count;
    int _divisor;
    int _parts_per_thread;
    int _custs_per_thread;
};


struct populate_one_part_input_t 
{
    int _partid;
};


struct populate_one_cust_input_t 
{
    int _custid;
};


EXIT_NAMESPACE(tpch);

#endif
