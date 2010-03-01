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

/** @file:   shore_tpcc_schema.cpp
 *
 *  @brief:  Declaration of the TPC-C tables
 *
 *  @author: Ippokratis Pandis, January 2008
 *
 */

#include "workload/tpcc/shore_tpcc_schema.h"

using namespace shore;

ENTER_NAMESPACE(tpcc);



/*********************************************************************
 *
 * TPC-C SCHEMA
 * 
 * This file contains the classes for tables in the TPC-C benchmark.
 * A class derived from tpcc_table_t (which inherits from table_desc_t) 
 * is created for each table in the databases.
 *
 *********************************************************************/


/*
 * indices created on the tables are:
 *
 * 1. WAREHOUSE
 * a. primary (unique) index on warehouse(w_id)
 *
 * 2. DISTRICT
 * a. primary (unique) index on district(d_id,d_w_id)
 *
 * 3. CUSTOMER
 * a. primary (unique) index on customer(c_w_id,c_d_id,c_id)
 * b. secondary index on customer(c_w_id,c_d_id,c_last,c_first,c_id)
 *
 * 4. NEW_ORDER
 * a. primary (unique) index on new_order(no_w_id,no_d_id,no_o_id)
 *
 * 5. ORDER
 * a. primary (unique) index on order(o_w_id,o_d_id,o_id)
 * b. secondary unique index on order(o_w_id,o_d_id,o_c_id,o_id) desc
 *
 * 6. ORDER_LINE
 * a. primary (unique) index on order_line(ol_w_id,ol_d_id,ol_o_id,ol_number)
 * 
 * 7. ITEM
 * a. primary (unique) index on item(i_id)
 *
 * 8. STOCK
 * a. primary (unique) index on stock(s_w_id,s_i_id)
 *
 */


warehouse_t::warehouse_t(string sysname) : 
    table_desc_t("WAREHOUSE", TPCC_WAREHOUSE_FCOUNT) 
{
    /* table schema */
    _desc[0].setup(SQL_INT,   "W_ID");
    _desc[1].setup(SQL_CHAR,  "W_NAME", 10);
    _desc[2].setup(SQL_CHAR,  "W_STREET1", 20);
    _desc[3].setup(SQL_CHAR,  "W_STREET2", 20);
    _desc[4].setup(SQL_CHAR,  "W_CITY", 20);   
    _desc[5].setup(SQL_CHAR,  "W_STATE", 2);
    _desc[6].setup(SQL_CHAR,  "W_ZIP", 9);
    _desc[7].setup(SQL_FLOAT, "W_TAX");   
    _desc[8].setup(SQL_FLOAT, "W_YTD");  /* DECIMAL(12,2) */

    int  keys[1] = { 0 }; // IDX { W_ID }

    // depending on the system name, create the corresponding indexes 

    // baseline - Regular 
    if (sysname.compare("baseline")==0) {
        TRACE( TRACE_DEBUG, "Regular idxs for (%s)\n", _name);
        // create unique index w_index on (w_id)
        create_primary_idx("W_INDEX", 0, keys, 1);
    }

#ifdef CFG_DORA
        // dora - NL indexes
        if (sysname.compare("dora")==0) {
            TRACE( TRACE_DEBUG, "NoLock idxs for (%s)\n", _name);

            // create unique index w_index on (w_id)
            // last param (nolock) is set to true
            create_primary_idx("W_INDEX_NL", 0, keys, 1, true);
        }
#endif

}


district_t::district_t(string sysname) : 
    table_desc_t("DISTRICT", TPCC_DISTRICT_FCOUNT) 
{
    /* table schema */
    _desc[0].setup(SQL_INT,   "D_ID");     
    _desc[1].setup(SQL_INT,   "D_W_ID");   
    _desc[2].setup(SQL_CHAR,  "D_NAME", 10);    /* VARCHAR(10) */
    _desc[3].setup(SQL_CHAR,  "D_STREET1", 20);
    _desc[4].setup(SQL_CHAR,  "D_STREET2", 20);
    _desc[5].setup(SQL_CHAR,  "D_CITY", 20);
    _desc[6].setup(SQL_CHAR,  "D_STATE", 2); 
    _desc[7].setup(SQL_CHAR,  "D_ZIP", 9);   
    _desc[8].setup(SQL_FLOAT, "D_TAX");    
    _desc[9].setup(SQL_FLOAT, "D_YTD");         /* DECIMAL(12,2) */
    _desc[10].setup(SQL_INT,  "D_NEXT_O_ID");

    int keys[2] = { 0, 1 }; // IDX { D_ID, D_W_ID }

    // baseline - Regular 
    if (sysname.compare("baseline")==0) {
        TRACE( TRACE_DEBUG, "Regular idxs for (%s)\n", _name); 
        // create unique index d_index on (d_id, w_id)
        create_primary_idx("D_INDEX", 0, keys, 2);
    }

#ifdef CFG_DORA
        // dora - NL indexes
        if (sysname.compare("dora")==0) {
            TRACE( TRACE_DEBUG, "NoLock idxs for (%s)\n", _name);

            // create unique index d_index on (d_id, w_id)
            // last param (nolock) is set to true
            create_primary_idx("D_INDEX_NL", 0, keys, 2, true);
        }
#endif

}

customer_t::customer_t(string sysname) : 
    table_desc_t("CUSTOMER", TPCC_CUSTOMER_FCOUNT) 
{
    /* table schema */
    _desc[0].setup(SQL_INT,    "C_ID");
    _desc[1].setup(SQL_INT,    "C_D_ID");       
    _desc[2].setup(SQL_INT,    "C_W_ID");       
    //_desc[5].setup(SQL_VARCHAR,   "C_FIRST", TPCC_C_FIRST_SZ);   
    _desc[3].setup(SQL_CHAR,   "C_FIRST", 16);  
    _desc[4].setup(SQL_CHAR,   "C_MIDDLE", 2);  
    _desc[5].setup(SQL_CHAR,   "C_LAST", 16);   
    //_desc[5].setup(SQL_VARCHAR,   "C_LAST", TPCC_C_LAST_SZ);   
    _desc[6].setup(SQL_CHAR,   "C_STREET1", 20);
    _desc[7].setup(SQL_CHAR,   "C_STREET2", 20);
    _desc[8].setup(SQL_CHAR,   "C_CITY", 20);   
    _desc[9].setup(SQL_CHAR,   "C_STATE", 2);   
    _desc[10].setup(SQL_CHAR,  "C_ZIP", 9);     
    _desc[11].setup(SQL_CHAR,  "C_PHONE", 16);  
    _desc[12].setup(SQL_FLOAT, "C_SINCE");           
    _desc[13].setup(SQL_CHAR,  "C_CREDIT", 2);  
    _desc[14].setup(SQL_FLOAT, "C_CREDIT_LIM");      
    _desc[15].setup(SQL_FLOAT, "C_DISCOUNT");
    _desc[16].setup(SQL_FLOAT, "C_BALANCE");         
    _desc[17].setup(SQL_FLOAT, "C_YTD_PAYMENT");     
    _desc[18].setup(SQL_FLOAT, "C_LAST_PAYMENT");    
    _desc[19].setup(SQL_INT,   "C_PAYMENT_CNT");     
    _desc[20].setup(SQL_CHAR,  "C_DATA_1", 250);
    _desc[21].setup(SQL_CHAR,  "C_DATA_2", 250);     

    int keys1[3] = {2, 1, 0 }; // IDX { C_W_ID, C_D_ID, C_ID }

    int keys2[5] = {2, 1, 5, 3, 0}; // IDX { C_W_ID, C_D_ID, C_LAST, C_FIRST, C_ID }

    // baseline - regular indexes
    if (sysname.compare("baseline")==0) {
        TRACE( TRACE_DEBUG, "Regular idxs for (%s)\n", _name);
 
        // create unique index c_index on (w_id, d_id, c_id)
        create_primary_idx("C_INDEX", 0, keys1, 3);
 
        // create index c_name_index on (w_id, d_id, last, first, id)
        create_index("C_NAME_INDEX", 0, keys2, 5, false);
    }

#ifdef CFG_DORA
        // dora - NL indexes
        if (sysname.compare("dora")==0) {
            TRACE( TRACE_DEBUG, "NoLock idxs for (%s)\n", _name);

            // create unique index c_index on (w_id, d_id, c_id)
            // last param (nolock) is set to true
            create_primary_idx("C_INDEX_NL", 0, keys1, 3, true);

            // create index c_name_index on (w_id, d_id, last, first, id)
            // last param (nolock) is set to true
            create_index("C_NAME_INDEX_NL", 0, keys2, 5, false, false, true);     
        }
#endif

}

history_t::history_t(string sysname) : 
    table_desc_t("HISTORY", TPCC_HISTORY_FCOUNT) 
{
    /* table schema */
    _desc[0].setup(SQL_INT,   "H_C_ID");
    _desc[1].setup(SQL_INT,   "H_C_D_ID");  
    _desc[2].setup(SQL_INT,   "H_C_W_ID"); 
    _desc[3].setup(SQL_INT,   "H_D_ID");   
    _desc[4].setup(SQL_INT,   "H_W_ID");    
    _desc[5].setup(SQL_FLOAT, "H_DATE");    
    _desc[6].setup(SQL_FLOAT, "H_AMOUNT");  
    _desc[7].setup(SQL_CHAR,  "H_DATA", 25); 

    // NO INDEXES
}


new_order_t::new_order_t(string sysname) : 
    table_desc_t("NEW_ORDER", TPCC_NEW_ORDER_FCOUNT) 
{
    /* table schema */
    _desc[0].setup(SQL_INT, "NO_O_ID");
    _desc[1].setup(SQL_INT, "NO_D_ID");
    _desc[2].setup(SQL_INT, "NO_W_ID");

    int keys[3] = {2, 1, 0}; // IDX { NO_W_ID, NO_D_ID, NO_O_ID }

    // baseline - Regular
    if (sysname.compare("baseline")==0) {
        TRACE( TRACE_DEBUG, "Regular idxs for (%s)\n", _name);
 
        // create unique index no_index on (w_id, d_id, o_id)
        create_primary_idx("NO_INDEX", 0, keys, 3);
    } 

#ifdef CFG_DORA
        // dora - NL indexes
        if (sysname.compare("dora")==0) {
            TRACE( TRACE_DEBUG, "NoLock idxs for (%s)\n", _name);

            // create unique index no_index on (w_id, d_id, o_id)
            // last param (nolock) is set to true
            create_primary_idx("NO_INDEX_NL", 0, keys, 3, true);        
        }
#endif

}


order_t::order_t(string sysname) : 
    table_desc_t("ORDER", TPCC_ORDER_FCOUNT) 
{
    /* table schema */
    _desc[0].setup(SQL_INT,   "O_ID");
    _desc[1].setup(SQL_INT,   "O_C_ID");       
    _desc[2].setup(SQL_INT,   "O_D_ID");       
    _desc[3].setup(SQL_INT,   "O_W_ID");       
    _desc[4].setup(SQL_FLOAT, "O_ENTRY_D");    
    _desc[5].setup(SQL_INT,   "O_CARRIER_ID"); 
    _desc[6].setup(SQL_INT,   "O_OL_CNT");   
    _desc[7].setup(SQL_INT,   "O_ALL_LOCAL");

    int keys1[3] = {3, 2, 0}; // IDX { O_W_ID, O_D_ID, O_ID }

    int keys2[4] = {3, 2, 1, 0}; // IDX { O_W_ID, O_D_ID, O_C_ID, O_ID }


    // baseline - regular indexes
    if (sysname.compare("baseline")==0) {
        TRACE( TRACE_DEBUG, "Regular idxs for (%s)\n", _name);
 
        // create unique index o_index on (w_id, d_id, o_id)
        create_index("O_INDEX", 0, keys1, 3);
             
        // create unique index o_cust_index on (w_id, d_id, c_id, o_id)
        create_index("O_CUST_INDEX", 0, keys2, 4);
    }

#ifdef CFG_DORA
        // dora - NL indexes
        if (sysname.compare("dora")==0) {
            TRACE( TRACE_DEBUG, "NoLock idxs for (%s)\n", _name);
            
            // create unique index o_index on (w_id, d_id, o_id)
            // last param (nolock) is set to true
            create_index("O_INDEX_NL", 0, keys1, 3, true, false, true);
            
            // create unique index o_cust_index on (w_id, d_id, c_id, o_id)
            // last param (nolock) is set to true
            create_index("O_CUST_INDEX_NL", 0, keys2, 4, true, false, true);
        }
#endif

}


order_line_t::order_line_t(string sysname) : 
    table_desc_t("ORDERLINE", TPCC_ORDER_LINE_FCOUNT) 
{
    /* table schema */
    _desc[0].setup(SQL_INT,    "OL_O_ID");
    _desc[1].setup(SQL_INT,    "OL_D_ID");      
    _desc[2].setup(SQL_INT,    "OL_W_ID");   
    _desc[3].setup(SQL_INT,    "OL_NUMBER"); 
    _desc[4].setup(SQL_INT,    "OL_I_ID");
    _desc[5].setup(SQL_INT,    "OL_SUPPLY_W_ID");  
    _desc[6].setup(SQL_FLOAT,  "OL_DELIVERY_D");   
    _desc[7].setup(SQL_INT,    "OL_QUANTITY");   
    _desc[8].setup(SQL_INT,    "OL_AMOUNT");
    _desc[9].setup(SQL_CHAR,   "OL_DIST_INFO", 25);

    int keys[4] = {2, 1, 0, 3}; // IDX { OL_W_ID, OL_D_ID, OL_O_ID, OL_NUMBER }

    // baseline - regular indexes
    if (sysname.compare("baseline")==0) {
        TRACE( TRACE_DEBUG, "Regular idxs for (%s)\n", _name);
 
        // create unique index ol_index on (w_id, d_id, o_id, ol_number)
        create_primary_idx("OL_INDEX", 10, keys, 4);
    }

#ifdef CFG_DORA
        // dora - NL indexes
        if (sysname.compare("dora")==0) {
            TRACE( TRACE_DEBUG, "NoLock idxs for (%s)\n", _name);

            // create unique index ol_index on (w_id, d_id, o_id, ol_number)
            // last param (nolock) is set to true
            create_primary_idx("OL_INDEX_NL", 10, keys, 4, true);
        }
#endif

}


item_t::item_t(string sysname) : 
    table_desc_t("ITEM", TPCC_ITEM_FCOUNT) 
{
    /* table schema */
    _desc[0].setup(SQL_INT,  "I_ID");
    _desc[1].setup(SQL_INT,  "I_IM_ID");
    _desc[2].setup(SQL_CHAR, "I_NAME", 24);   
    _desc[3].setup(SQL_INT,  "I_PRICE");
    _desc[4].setup(SQL_CHAR, "I_DATA", 50);
	
    int keys[1] = {0}; // IDX { I_ID }

    // baseline - regular indexes
    if (sysname.compare("baseline")==0) {
        TRACE( TRACE_DEBUG, "Regular idxs for (%s)\n", _name);
             
        // create unique index on i_index on (i_id)
        create_primary_idx("I_INDEX", 0, keys, 1);
    } 

#ifdef CFG_DORA
        // dora - NL indexes
        if (sysname.compare("dora")==0) {
            TRACE( TRACE_DEBUG, "NoLock idxs for (%s)\n", _name);

            // create unique index on i_index on (i_id)
            // last param (nolock) is set to true        
            create_primary_idx("I_INDEX_NL", 0, keys, 1, true);
        }
#endif

}


stock_t::stock_t(string sysname) : 
    table_desc_t("STOCK", TPCC_STOCK_FCOUNT) 
{
    /* table schema */
    _desc[0].setup(SQL_INT,    "S_I_ID");
    _desc[1].setup(SQL_INT,    "S_W_ID");       
    _desc[2].setup(SQL_INT,    "S_REMOTE_CNT");
    _desc[3].setup(SQL_INT,    "S_QUANTITY");  
    _desc[4].setup(SQL_INT,    "S_ORDER_CNT"); 
    _desc[5].setup(SQL_INT,    "S_YTD");       
    _desc[6].setup(SQL_CHAR,   "S_DIST0", 24); 
    _desc[7].setup(SQL_CHAR,   "S_DIST1", 24);  
    _desc[8].setup(SQL_CHAR,   "S_DIST2", 24);  
    _desc[9].setup(SQL_CHAR,   "S_DIST3", 24);  
    _desc[10].setup(SQL_CHAR,  "S_DIST4", 24);  
    _desc[11].setup(SQL_CHAR,  "S_DIST5", 24);  
    _desc[12].setup(SQL_CHAR,  "S_DIST6", 24);  
    _desc[13].setup(SQL_CHAR,  "S_DIST7", 24);  
    _desc[14].setup(SQL_CHAR,  "S_DIST8", 24);  
    _desc[15].setup(SQL_CHAR,  "S_DIST9", 24);
    _desc[16].setup(SQL_CHAR,  "S_DATA", 50); 

    int keys[2] = { 0, 1 }; // IDX { S_W_ID, S_I_ID }

    // baseline - regular indexes
    if (sysname.compare("baseline")==0) {
        TRACE( TRACE_DEBUG, "Regular idxs for (%s)\n", _name);
 
        // create unique index s_index on (w_id, i_id)
        create_primary_idx("S_INDEX", 0, keys, 2);
    } 

#ifdef CFG_DORA
        // dora - NL indexes
        if (sysname.compare("dora")==0) {
            TRACE( TRACE_DEBUG, "NoLock idxs for (%s)\n", _name);
            
            // create unique index s_index on (w_id, i_id)
            // last param (nolock) is set to true
            create_primary_idx("S_INDEX_NL", 0, keys, 2, true);
        }
#endif

}


EXIT_NAMESPACE(tpcc);