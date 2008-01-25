/* -*- mode:C++; c-basic-offset:4 -*- */

/** @file payment_functions.cpp
 *
 *  @brief Implementation of common functionality among 
 *  PAYMENT_BASELINE and PAYMENT_STAGED transactions
 *
 *  @author Ippokratis Pandis (ipandis)
 */


#include "stages/tpcc/common/trx_packet.h"
#include "workload/tpcc/tpcc_env.h"
#include "stages/tpcc/bdb/payment_functions.h"

using namespace qpipe;


ENTER_NAMESPACE(tpcc_payment);


/** Forward declaration of helper functions */

void updateCustomerData(tpcc_customer_tuple* a_customer);


/** Exported Functions */



/** @fn insertHistory
 *  
 *  @brief Step 5: Insert a new HISTORY row. A new row is inserted into the HISTORY
 *  table with H_C_ID = C_ID, H_C_D_ID = C_D_ID, H_C_W_ID = C_W_ID, H_D_ID = D_ID,
 *  H_W_ID = W_ID and H_DATA = concatenating W_NAME and D_NAME separated by
 *  4 spaces.
 *
 *  @brief Selects which updateCustomerX() function to call
 *
 *  @return 0 on success, non-zero on failure
 */

int insertHistory(payment_input_t* pin, DbTxn* txn, s_payment_dbt_t* a_p_dbts) {

    /////////////////////////////
    ///// START INS_HISTORY /////
    
    TRACE( TRACE_TRX_FLOW, "Step 5: Inserting a new row in HISTORY table\n");
    
    // Preparing HISTORY tuple
    tpcc_history_tuple ht;
    ht.H_C_ID = pin->_c_id;
    ht.H_C_D_ID = pin->_home_d_id;
    ht.H_C_W_ID = pin->_home_wh_id;
    ht.H_D_ID = pin->_home_d_id;
    ht.H_W_ID = pin->_home_wh_id;       
    ht.H_DATE = pin->_h_date;
    ht.H_AMOUNT = pin->_h_amount;    
    
    // FIXME (ip) Modification of the specification. Instead of concatenating
    // W_NAME and D_NAME we do that with W_ID and D_ID
    snprintf(&ht.H_DATA[0], 25, "%d    %d", 
             pin->_home_wh_id, 
             pin->_home_d_id);
    ht.H_DATA[25] = '\0';


    // HISTORY has a key comprised by the first 6 integers
    tpcc_history_tuple_key hk;
    hk.H_C_ID = pin->_c_id;
    hk.H_C_D_ID = pin->_home_d_id;
    hk.H_C_W_ID = pin->_home_wh_id;
    hk.H_D_ID = pin->_home_d_id;
    hk.H_W_ID = pin->_home_wh_id;       
    hk.H_DATE = pin->_h_date;


    // prepare to insert
    Dbt key_h(&hk, sizeof(hk));
    Dbt data_h(&ht, sizeof(ht));
    
    return (tpcc_tables[TPCC_TABLE_HISTORY].db->put(txn, &key_h, &data_h, 0));

    ///// EOF INS_HISTORY /////
    ///////////////////////////
}



/** @fn updateCustomer
 *  
 *  @brief Selects which updateCustomerX() function to call
 *
 *  @return 0 on success, non-zero on failure
 */

int updateCustomer(payment_input_t* pin, DbTxn* txn, s_payment_dbt_t* a_p_dbts) {

    //////////////////////////////
    ///// START UPD_CUSTOMER /////

    // FIXME (ip) : It always chooses the customer by id. It should be 40.
    if (pin->_v_cust_ident_selection < 0) {
        //       if (pin->_v_cust_ident_selection < 40) {
        
        /** Step 4b: Retrieve CUSTOMER based on C_LAST */
        TRACE( TRACE_TRX_FLOW, 
               "Step 4b: Retrieving and Updating the row in the CUSTOMER table " \
               "with matching C_W_ID=%d and C_D_ID=%d and C_LAST=%s\n",
               pin->_home_wh_id,
               pin->_home_d_id, 
               pin->_c_last);
        
        return (updateCustomerByLast(txn, 
                                     pin->_home_wh_id, 
                                     pin->_home_d_id, 
                                     pin->_c_last, 
                                     pin->_h_amount,
                                     a_p_dbts));
    }
    else {
        /** Step 4a: Retrieve CUSTOMER based on C_ID */
        TRACE( TRACE_TRX_FLOW, 
               "Step 4a: Retrieving and Updating the row in the CUSTOMER table " \
               "with matching C_W_ID=%d and C_D_ID=%d and C_ID=%d\n",
               pin->_home_wh_id,
               pin->_home_d_id, 
               pin->_c_id);
        
        return (updateCustomerByID(txn, 
                                   pin->_home_wh_id, 
                                   pin->_home_d_id, 
                                   pin->_c_id, 
                                   pin->_h_amount,
                                   a_p_dbts));
    }

    // It should never reach this point, return error
    return (1);
    
    ///// EOF UPD_CUSTOMER /////
    ////////////////////////////
}



/** @fn updateDistrict
 *  
 *  @brief Step 3: Retrieve the row in the DISTRICT table with matching D_W_ID and D_ID.
 *  D_NAME, D_STREET_1, D_STREET_2, D_CITY, D_STATE and D_ZIP are retrieved.
 *  D_YTD is increased by H_AMOUNT.
 *
 *  @return 0 on success, non-zero on failure
 */

int updateDistrict(payment_input_t* pin, DbTxn* txn, s_payment_dbt_t* a_p_dbts) {

    //////////////////////////////
    ///// START UPD_DISTRICT /////
    
    TRACE( TRACE_TRX_FLOW, 
           "Step 3: Updating the row in the DISTRICT table with matching " \
           "D_ID=%d and D_W_ID=%d\n", 
           pin->_home_d_id, 
           pin->_home_wh_id);
    
    // DISTRICT key: D_ID, D_W_ID 
    tpcc_district_tuple_key dk;
    dk.D_ID = pin->_home_d_id;
    dk.D_W_ID = pin->_home_wh_id;
    //    dk.D_PADDING_KEY = 0;
    Dbt key_d(&dk, sizeof(dk));

    
    if (tpcc_tables[TPCC_TABLE_DISTRICT].db->get(txn, &key_d, &a_p_dbts->distData, DB_RMW) ==
        DB_NOTFOUND)
        {
            THROW3(BdbException, 
                   "district with id=(%d,%d) not found in the database\n", 
                   dk.D_ID,
                   dk.D_W_ID);

            return (1);
        }
    
    
    tpcc_district_tuple* district = (tpcc_district_tuple*)a_p_dbts->distData.get_data();
    
    assert(district); // make sure that we got a district
    
    district->D_YTD += pin->_h_amount;

    return (tpcc_tables[TPCC_TABLE_DISTRICT].db->put(txn, &key_d, &a_p_dbts->distData, 0));
    
    ///// EOF UPD_DISTRICT /////
    ////////////////////////////
}



/** @fn updateWarehouse
 *
 *  @brief Step 2: Retrieve the row in the WAREHOUSE table with matching W_ID.
 *  W_NAME, W_STREET_1, W_STREET_2, W_CITY, W_STATE and W_ZIP are retrieved.
 *  W_YTD is increased by H_AMOUNT.
 *  
 *  @return 0 on success, non-zero on failure
 */

int updateWarehouse(payment_input_t* pin, DbTxn* txn, s_payment_dbt_t* a_p_dbts) {

    ///////////////////////////////
    ///// START UPD_WAREHOUSE /////    
        
    TRACE( TRACE_TRX_FLOW, 
           "Step 2: Updating the row in the WAREHOUSE table with matching W_ID=%d\n", 
           pin->_home_wh_id );
    
    // WAREHOUSE key: W_ID 
    tpcc_warehouse_tuple_key wk;
    wk.W_ID = pin->_home_wh_id;
    //    wk.W_PADDING_KEY = 0;
    Dbt key_wh(&wk, sizeof(wk));
    
    if (tpcc_tables[TPCC_TABLE_WAREHOUSE].db->get(txn, &key_wh, &a_p_dbts->whData, DB_RMW) == 
        DB_NOTFOUND) 
        {
            THROW2(BdbException, 
                   "warehouse with id=%d not found in the database\n", 
                   wk.W_ID);
            return (1);
        }
    
    tpcc_warehouse_tuple* warehouse = (tpcc_warehouse_tuple*)a_p_dbts->whData.get_data();
    
    assert(warehouse); // make sure that we got a warehouse
    
    warehouse->W_YTD += pin->_h_amount;
    
    return (tpcc_tables[TPCC_TABLE_WAREHOUSE].db->put(txn, &key_wh, &a_p_dbts->whData, 0));

    ///// EOF UPD_WAREHOUSE //////
    //////////////////////////////
}



/** @fn updateCustomerByID
 *
 *  @brief Step 4, Case 1: The customer is selected based on customer number.
 *  That is, the row in the CUSTOMER table with matching C_W_ID, C_D_ID and C_ID
 *  is selected. C_FIRST, C_MIDDLE, C_LAST, C_STREET_1, C_STREET_2, C_CITY, C_STATE,
 *  C_ZIP, C_PHONE, C_SINCE, C_CREDIT, C_CREDIT_LIM, C_DISCOUNT, C_BALANCE, C_DATA_1,
 *  and C_DATA_2 are retrieved.
 *  C_BALANCE is decreased by H_AMOUNT. 
 *  C_YTD_PAYMENT is increased by H_AMOUNT. 
 *  C_PAYMENT_CNT is increased by 1.
 *  updateCustomerData is called.
 *  
 *  @return 0 on success, non-zero on failure
 */
    
int updateCustomerByID(DbTxn* txn, 
                       int wh_id, 
                       int d_id, 
                       int c_id, 
                       decimal h_amount,
                       s_payment_dbt_t* a_p_dbts) 
{
    assert (txn); // abort if no valid transaction handle

    tpcc_customer_tuple_key ck;
    ck.C_C_ID = c_id;
    ck.C_D_ID = d_id;
    ck.C_W_ID = wh_id;
    Dbt key_c(&ck, sizeof(ck));
    
    if (tpcc_tables[TPCC_TABLE_CUSTOMER].db->get(txn, &key_c, &a_p_dbts->custData, DB_RMW) ==
        DB_NOTFOUND)
        {
            THROW4( BdbException,
                   "Customer with id=(%d,%d,%d) not found in the database\n", 
                   ck.C_C_ID,
                   ck.C_D_ID,
                   ck.C_W_ID);
        }
    
    tpcc_customer_tuple* customer = (tpcc_customer_tuple*)a_p_dbts->custData.get_data();
    
    assert(customer); // make sure that we got a customer

    TRACE( TRACE_RECORD_FLOW, 
           "\nBal: (%.2f) Payment: (%.2f) Cnt: (%d) Cred: (%s) Amount: (%.2f)\n",
           customer->C_BALANCE.to_double(),
           customer->C_YTD_PAYMENT.to_double(),
           customer->C_PAYMENT_CNT,
           customer->C_CREDIT,
           h_amount.to_double());


    // updating customer data
    customer->C_BALANCE -= h_amount;
    customer->C_YTD_PAYMENT += h_amount;
    customer->C_PAYMENT_CNT++;

    TRACE( TRACE_RECORD_FLOW, 
           "\nBal: (%.2f) Payment: (%.2f) Cnt: (%d) Cred: (%s) Amount: (%.2f)\n",
           customer->C_BALANCE.to_double(),
           customer->C_YTD_PAYMENT.to_double(),
           customer->C_PAYMENT_CNT,
           customer->C_CREDIT,
           h_amount.to_double());
    
    
    if (strncmp(customer->C_CREDIT, "BC", 2) == 0) {
        updateCustomerData(customer);
    }
    
    return (tpcc_tables[TPCC_TABLE_CUSTOMER].db->put(txn, &key_c, &a_p_dbts->custData, 0));
}




/** @fn updateCustomerByLast
 *
 *  @brief Step 4, Case 2: The customer is selected based on customer last name.
 *  That is, all rows in the CUSTOMER table with matching C_W_ID, C_D_ID and C_LAST
 *  are selected sorted by C_FIRST in ascending order. Let (n) be the number of rows
 *  selected. C_ID, C_FIRST, C_MIDDLE, C_LAST, C_STREET_1, C_STREET_2, C_CITY, C_STATE,
 *  C_ZIP, C_PHONE, C_SINCE, C_CREDIT, C_CREDIT_LIM, C_DISCOUNT, C_BALANCE, C_DATA_1,
 *  and C_DATA_2 are retrieved from the row at position (n/2 rounded up to the next
 *  integer) in the sorted set of selected rows.
 *  C_BALANCE is decreased by H_AMOUNT. 
 *  C_YTD_PAYMENT is increased by H_AMOUNT. 
 *  C_PAYMENT_CNT is increased by 1.
 *  updateCustomerData is called.
 *  
 *  @return 0 on success, non-zero on failure
 */
    
int updateCustomerByLast(DbTxn* txn, 
                         int wh_id, 
                         int d_id, 
                         char* c_last,
                         decimal h_amount,
                         s_payment_dbt_t* a_p_dbts) 
{
    assert(1==0); // FIXME (ip) Not implemented yet

    assert(txn); // abort if no valid transaction handle 

    int iResult = 0;

    tpcc_customer_tuple a_customer;
    
    TRACE( TRACE_ALWAYS, "*** TO BE DONE! Updating Customer using C_LAST\n");

    
    /** Retrieving CUSTOMER rows using C_LAST (and a cursor) */
    
    if (strncmp(a_customer.C_CREDIT, "BC", 2) == 0) {
        updateCustomerData(&a_customer);
    } 
   
    return (iResult);
}
    
    
/** @fn updateCustomerData
 *
 *  @brief If the value of C_CREDIT is equal to "BC", then the following history
 *  information: C_ID, C_D_ID, C_W_ID, D_ID, W_ID, and H_AMOUNT, are inserted at
 *  the left of C_DATA field by shifting the existing content of C_DATA to the right
 *  by an equal number of bytes and by discarding the bytes that are shifted out of
 *  the right side of the C_DATA field. The content of the C_DATA field never exceeds
 *  500 characters. The selected customer is updated with the new C_DATA field.
 */
    
void updateCustomerData(tpcc_customer_tuple* a_customer) 
{
    assert( a_customer ); // make sure correct parameters
    assert( strncmp(a_customer->C_CREDIT, "BC", 2) == 0 );

    // FIXME (ip) Modification instead of writing D_ID, W_ID, and H_AMOUNT
    // we write again C_D_ID, C_W_ID, and C_BALANCE

    char tmp [STRSIZE(500)];

    // writes new info
    sprintf(&tmp[0], "%d:%d:%d:%d:%d:%.2f|",
             a_customer->C_C_ID,
             a_customer->C_D_ID,
             a_customer->C_W_ID,
             a_customer->C_D_ID,
             a_customer->C_W_ID,
             a_customer->C_BALANCE.to_double());

    int i = strlen(tmp);

    // copies first half of old data, which is stored 
    // on the first member variable
    strncpy(&tmp[i], a_customer->C_DATA_1, 250);

    // copies part of the second half of old data, which
    // is stored on the second member variable
    strncpy(&tmp[i+250], a_customer->C_DATA_2, 250-i);

    TRACE( TRACE_RECORD_FLOW,
           "Before\nDATA1 - %s\nDATA2 - %s\n",
           a_customer->C_DATA_1,
           a_customer->C_DATA_2);

    // copy back to the structure
    strncpy(a_customer->C_DATA_1, tmp, 250);
    a_customer->C_DATA_1[250] = '\0';
    strncpy(a_customer->C_DATA_2, &tmp[250], 250);
    a_customer->C_DATA_2[250] = '\0';


    TRACE( TRACE_RECORD_FLOW,
           "After\nDATA1 - %s\nDATA2 - %s\n",
           a_customer->C_DATA_1,
           a_customer->C_DATA_2);
}



/** @fn executePaymentBaseline
 *
 *  @brief Executes the PAYMENT transaction serially. Wrapper function.
 *  
 *  @return A trx_result_tuple_t with the status of the transaction.
 */
    
trx_result_tuple_t executePaymentBaseline(payment_input_t* pin,
                                          const int id,
                                          DbTxn* txn,
                                          s_payment_dbt_t* a_dbts)
{        
    // initialize the result structure
    trx_result_tuple_t aTrxResultTuple(UNDEF, id);
    
    try {
        
       TRACE( TRACE_TRX_FLOW, "*** EXECUTING TRX CONVENTIONALLY ***\n");
    
       /** @note PAYMENT TRX According to TPC-C benchmark, Revision 5.8.0 pp. 32-35 */

    
       /** Step 1: The database transaction is started */
       TRACE( TRACE_TRX_FLOW, "Step 1: The database transaction is started\n" );
       dbenv->txn_begin(NULL, &txn, 0);

       if (updateWarehouse(pin, txn, a_dbts)) {
           
           // Failed. Throw exception
           THROW1( BdbException, 
                   "WAREHOUSE update failed...\n");
       }


       if (updateDistrict(pin, txn, a_dbts)) {
           
           // Failed. Throw exception
           THROW1( BdbException, 
                   "DISTRICT update failed...\n");
       }


       if (updateCustomer(pin, txn, a_dbts)) {           
           // Failed. Throw exception
           THROW1( BdbException,
                   "CUSTOMER update failed...\n");
       }

       if (insertHistory(pin, txn, a_dbts)) {

           // Failed. Throw exception
           THROW1( BdbException, 
                   "HISTORY instertion failed...\n");
       }


    
       /** Step 6: The database transaction is committed */
       TRACE( TRACE_TRX_FLOW, "Step 6: The database transaction is committed\n" );
       txn->commit(0);
    }
    catch(DbException err) {
        dbenv->err(err.get_errno(), "%d: Caught exception\n");
        TRACE( TRACE_ALWAYS, "DbException - Aborting PAYMENT trx...\n");
        
        txn->abort();
	    
        aTrxResultTuple.set_state(ROLLBACKED);
        return (aTrxResultTuple);
        
        // FIXME (ip): We may want to have retries
        //if (++failed_tries > MAX_TRIES) {
        //   txn->abort();
        //   TRACE( TRACE_ALWAYS, "MAX_FAILS - Aborting...\n");        
    }
    catch(...) {
        TRACE( TRACE_ALWAYS, "Unknown Exception - Aborting PAYMENT trx...\n");
        
        txn->abort();
	    
        aTrxResultTuple.set_state(ROLLBACKED);
        return (aTrxResultTuple);
    }

    // if reached this point transaction succeeded
    aTrxResultTuple.set_state(COMMITTED);
    return (aTrxResultTuple);

} // EOF: executePaymentBaseline



//----------------------------------------
// @class s_payment_dbt_t
//
// @brief Dbts for the payment transaction
//


/** @fn allocate_payment_dbts
 *
 *  @brief Allocates the required dbts for the PAYMENT_* transactions
 *  and sets their flag as memory allocated by user.
 */

void s_payment_dbt_t::allocate() {

    /** @note The caller (tpcc_payment_*_driver) is the owner of
     *  those dbts
     */

    memset(&whData, 0, sizeof(Dbt));
    whData.set_data(malloc(sizeof(tpcc_warehouse_tuple)));
    whData.set_ulen(sizeof(tpcc_warehouse_tuple));
    whData.set_flags(DB_DBT_USERMEM);


    memset(&distData, 0, sizeof(Dbt));
    distData.set_data(malloc(sizeof(tpcc_district_tuple)));
    distData.set_ulen(sizeof(tpcc_district_tuple));
    distData.set_flags(DB_DBT_USERMEM);


    memset(&custData, 0, sizeof(Dbt));
    custData.set_data(malloc(sizeof(tpcc_customer_tuple)));
    custData.set_ulen(sizeof(tpcc_customer_tuple));
    custData.set_flags(DB_DBT_USERMEM);

    _allocated = 1;
}


/** @fn deallocate_payment_dbts
 *
 *  @brief Deallocates the required dbts for the PAYMENT_* transactions
 */

void s_payment_dbt_t::deallocate() {

    if (whData.get_data())
        delete ((tpcc_warehouse_tuple*)whData.get_data());

    if (distData.get_data())
        delete ((tpcc_district_tuple*)distData.get_data());
    
    if (custData.get_data())
        delete ((tpcc_customer_tuple*)custData.get_data());

    _allocated = 0;
}



/** @fn reset_payment_dbts
 *
 *  @brief Resets the required dbts for the PAYMENT_* transactions
 */

void s_payment_dbt_t::reset() {

    TRACE( TRACE_DEBUG, "resetting...\n");
    // FIXME (ip) Do we really need to do anything here?
}



EXIT_NAMESPACE(tpcc_payment);
