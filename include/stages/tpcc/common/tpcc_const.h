/* -*- mode:C++; c-basic-offset:4 -*- */

/** @file:   tpcc_const.h
 *
 *  @brief:  Constants needed by the TPC-C kit
 *
 *  @author: Mengzhi Wang, April 2001
 *  @author: Ippokratis Pandis, January 2008
 *
 */

#ifndef __TPCC_CONST_H
#define __TPCC_CONST_H


#include "util/namespace.h"


ENTER_NAMESPACE(tpcc);


/* ----------------------------- */
/* --- TPC-C SCALING FACTORS --- */
/* ----------------------------- */



/* parameters used to estimate the required storage
 * set it to 4 or 5 for extremely small databases and 2 for
 * databases with standard scales. */

const int   STORAGE_FACTOR           = 2;


/* Scale-down version 
#define DISTRICTS_PER_WAREHOUSE                 4
#define CUSTOMERS_PER_DISTRICT                  120
#define ITEMS                                   4000
#define STOCK_PER_WAREHOUSE                     ITEMS
// #define MIN_OL_PER_ORDER                        4
// #define MAX_OL_PER_ORDER                        8
#define MIN_OL_PER_ORDER                        5
#define MAX_OL_PER_ORDER                        15
#define NU_ORDERS_PER_DISTRICT                  36
*/
 
/* Scale-down version 2) 
#define DISTRICTS_PER_WAREHOUSE                 4
#define CUSTOMERS_PER_DISTRICT                  300
#define ITEMS                                   10000
#define STOCK_PER_WAREHOUSE                     ITEMS
#define MIN_OL_PER_ORDER                        4
#define MAX_OL_PER_ORDER                        8
#define NU_ORDERS_PER_DISTRICT                  90
*/


/* --- standard scale -- */

const int DISTRICTS_PER_WAREHOUSE = 10;
const int CUSTOMERS_PER_DISTRICT  = 3000;
const int ITEMS                   = 100000;
const int STOCK_PER_WAREHOUSE     = ITEMS;
const int MIN_OL_PER_ORDER        = 5;
const int MAX_OL_PER_ORDER        = 15;
const int NU_ORDERS_PER_DISTRICT  = 900;

const int MAX_TABLENAM_LENGTH     = 20;
const int MAX_RECORD_LENGTH       = 512;


/* --- number of fields per table --- */

const int TPCC_WAREHOUSE_FCOUNT  = 9;
const int TPCC_DISTRICT_FCOUNT   = 11;
const int TPCC_CUSTOMER_FCOUNT   = 22;
const int TPCC_HISTORY_FCOUNT    = 8;
const int TPCC_NEW_ORDER_FCOUNT  = 3;
const int TPCC_ORDER_FCOUNT      = 8;
const int TPCC_ORDER_LINE_FCOUNT = 10;
const int TPCC_ITEM_FCOUNT       = 5;
const int TPCC_STOCK_FCOUNT      = 17;



/* ------------------------ */
/* --- TPC-C DATA FILES --- */
/* ------------------------ */

/* --- table ids --- */

const int TBL_ID_WAREHOUSE     = 0;
const int TBL_ID_DISTRICT      = 1;
const int TBL_ID_CUSTOMER      = 2;
const int TBL_ID_HISTORY       = 3;
const int TBL_ID_ITEM          = 4;
const int TBL_ID_NEW_ORDER     = 5;
const int TBL_ID_ORDER         = 6;
const int TBL_ID_ORDERLINE     = 7;
const int TBL_ID_STOCK         = 8;

const int SHORE_PAYMENT_TABLES = 4;
const int SHORE_TPCC_TABLES    = 9;


/* --- location of tables --- */

#define SHORE_TPCC_DATA_DIR        "tpcc_sf"

#define SHORE_TPCC_DATA_WAREHOUSE  "WAREHOUSE.dat"
#define SHORE_TPCC_DATA_DISTRICT   "DISTRICT.dat"
#define SHORE_TPCC_DATA_CUSTOMER   "CUSTOMER.dat"
#define SHORE_TPCC_DATA_HISTORY    "HISTORY.dat"
#define SHORE_TPCC_DATA_ITEM       "ITEM.dat"
#define SHORE_TPCC_DATA_NEW_ORDER  "NEW_ORDER.dat"
#define SHORE_TPCC_DATA_ORDER      "ORDER.dat"
#define SHORE_TPCC_DATA_ORDERLINE  "ORDERLINE.dat"
#define SHORE_TPCC_DATA_STOCK      "STOCK.dat"



/* ----------------------------- */
/* --- TPC-C TRANSACTION MIX --- */
/* ----------------------------- */

const int XCT_NEW_ORDER     = 1;
const int XCT_PAYMENT       = 2;
const int XCT_ORDER_STATUS  = 3;
const int XCT_DELIVERY      = 4;
const int XCT_STOCK_LEVEL   = 5;

const int XCT_DORA_NEW_ORDER     = 101;
const int XCT_DORA_PAYMENT       = 102;
const int XCT_DORA_ORDER_STATUS  = 103;
const int XCT_DORA_DELIVERY      = 104;
const int XCT_DORA_STOCK_LEVEL   = 105;


/* --- probabilities for the TPC-C MIX --- */

const int PROB_NEWORDER     = 45;
const int PROB_PAYMENT      = 43;
const int PROB_ORDER_STATUS = 4;
const int PROB_DELIVERY     = 4;
const int PROB_STOCK_LEVEL  = 4;


/* --- Helper functions --- */


/* --- translates or picks a random xct type given the benchmark specification --- */

int random_xct_type(int selected);


EXIT_NAMESPACE(tpcc);

#endif /* __TPCC_CONST_H */
