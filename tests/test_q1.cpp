// -*- mode:C++ c-basic-offset:4 -*-

/** @file    : test_q6.cpp
 *  @brief   : Unittest for Q1
 *  @version : 0.1
 *  @history :
 6/9/2006: Initial version
*/ 

#include "thread.h"
#include "stage_container.h"
#include "trace/trace.h"
#include "qpipe_panic.h"
#include "dispatcher.h"
#include "tester_thread.h"
#include "util/time_util.h"
#include "stages/tscan.h"
#include "stages/aggregate.h"
#include "stages/sort.h"
#include "stages/fscan.h"
#include "stages/fdump.h"
#include "stages/merge.h"

#include <unistd.h>
#include <sys/time.h>
#include <math.h>

using namespace qpipe;

extern uint32_t trace_current_setting;


// Q1 SPECIFIC UTILS
/* Declaration of some constants */

# define DATABASE_HOME	 "."
# define CONFIG_DATA_DIR "./database"
# define TMP_DIR "./temp"

# define TABLE_LINEITEM_NAME "LINEITEM"
# define TABLE_LINEITEM_ID   "TBL_LITEM"

/* Set Bufferpool equal to 450 MB -- Maximum is 4GB in 32-bit platforms */
size_t TPCH_BUFFER_POOL_SIZE_GB = 0; /* 0 GB */
size_t TPCH_BUFFER_POOL_SIZE_BYTES = 450 * 1024 * 1024; /* 450 MB */


/* tpch_l_shipmode.
   TODO: Unnecessary */
enum tpch_l_shipmode {
    REG_AIR,
    AIR,
    RAIL,
    TRUCK,
    MAIL,
    FOB,
    SHIP
};


/* Lineitem tuple.
   TODO: Catalog will provide this metadata info */
struct tpch_lineitem_tuple {
    int L_ORDERKEY;
    int L_PARTKEY;
    int L_SUPPKEY;
    int L_LINENUMBER;
    double L_QUANTITY;
    double L_EXTENDEDPRICE;
    double L_DISCOUNT;
    double L_TAX;
    char L_RETURNFLAG;
    char L_LINESTATUS;
    time_t L_SHIPDATE;
    time_t L_COMMITDATE;
    time_t L_RECEIPTDATE;
    char L_SHIPINSTRUCT[25];
    tpch_l_shipmode L_SHIPMODE;
    // char L_COMMENT[44];
};


int tpch_lineitem_bt_compare_fcn(Db*, const Dbt* k1, const Dbt* k2) {
    // LINEITEM key has 3 integers
    int u1[3];
    int u2[3];
    memcpy(u1, k1->get_data(), 3 * sizeof(int));
    memcpy(u2, k2->get_data(), 3 * sizeof(int));

    if ((u1[0] < u2[0]) || ((u1[0] == u2[0]) && (u1[1] < u2[1])) || ((u1[0] == u2[0]) && (u1[1] == u2[1]) && (u1[2] < u2[2])))
	return -1;
    else if ((u1[0] == u2[0]) && (u1[1] == u2[1]) && (u1[2] == u2[2]))
	return 0;
    else
	return 1;

}


/** @fn    : datestr_to_timet(char*)
 *  @brief : Converts a string to corresponding time_t
 */

time_t datestr_to_timet(char* str) {
    char buf[100];
    strcpy(buf, str);

    // str in yyyy-mm-dd format
    char* year = buf;
    char* month = buf + 5;
    // char* day = buf + 8;

    buf[4] = '\0';
    buf[7] = '\0';

    tm time_str;
    time_str.tm_year = atoi(year) - 1900;
    time_str.tm_mon = atoi(month) - 1;
    time_str.tm_mday = 4;
    time_str.tm_hour = 0;
    time_str.tm_min = 0;
    time_str.tm_sec = 1;
    time_str.tm_isdst = -1;

    return mktime(&time_str);
}


// END OF: Q1 SPECIFIC UTILS


// Q1 TSCAN FILTER 

/* Specific filter for this client */

class q1_tscan_filter_t : public tuple_filter_t {

private:
  time_t t;

  struct timeval tv;
  uint mv;

  /* Random Predicates */
  /* TPC-H Specification 2.3.0 */

  /* DELTA random within [60 .. 120] */
  int DELTA;

public:

    /* Initialize the predicates */
    q1_tscan_filter_t() : tuple_filter_t(sizeof(tpch_lineitem_tuple)) {
	t = datestr_to_timet("1998-12-01");

	/* Calculate random predicates */
	gettimeofday(&tv, 0);
	mv = tv.tv_usec * getpid();

	DELTA = 60 + abs((int)(60*(float)(rand_r(&mv))/(float)(RAND_MAX+1)));

	// Predicate: 1998-12-01 - DELTA days
	t = time_add_day(t, -DELTA);
    }


    /* Predication */
    virtual bool select(const tuple_t &input) {

	/* Predicate:
	   L_SHIPDATE <= 1998-12-01 - DELTA DAYS
	*/

	tpch_lineitem_tuple *tuple = (tpch_lineitem_tuple*)input.data;

	if  ( tuple->L_SHIPDATE >= t ) {
	    //printf("+");
	    return (true);
	}
	else {
	    //printf(".");
	    return (false);
	}
    }
    
    /* Projection */
    virtual void project(tuple_t &dest, const tuple_t &src) {

	/* Should project L_EXTENDEDPRICE & L_DISCOUNT */

	// Calculate L_EXTENDEDPRICE
	tpch_lineitem_tuple *at = (tpch_lineitem_tuple*)(src.data);

	// PADDING
	int dest_offset = 0;
	memcpy(dest.data + dest_offset, &at->L_RETURNFLAG, sizeof(char));

	dest_offset += sizeof(char);
	memcpy(dest.data + dest_offset, &at->L_LINESTATUS, sizeof(char));

	int pad_offset = sizeof(int) - 2*sizeof(char);
	char padding[pad_offset];
	dest_offset += pad_offset;
	memcpy(dest.data + dest_offset, &padding, pad_offset);

	dest_offset += sizeof(double);
	memcpy(dest.data + dest_offset, &at->L_QUANTITY, sizeof(double));

	dest_offset += sizeof(double);
	memcpy(dest.data + dest_offset, &at->L_EXTENDEDPRICE, sizeof(double));

	dest_offset += sizeof(double);
	memcpy(dest.data + dest_offset, &at->L_DISCOUNT, sizeof(double));

	dest_offset += sizeof(double);
	memcpy(dest.data + dest_offset, &at->L_TAX, sizeof(double));       
    }
};


// END OF: Q1 TSCAN FILTER


// Q1 SORT

struct int_tuple_comparator_t : public tuple_comparator_t {
    virtual int compare(const key_tuple_pair_t &a, const key_tuple_pair_t &b) {
        return a.key - b.key;
    }
    virtual int make_key(const tuple_t &tuple) {
        return *(int*)tuple.data;
    }
};

// END OF: Q1 SORT


// Q6 AGG

class count_aggregate_t : public tuple_aggregate_t {

private:
    // STATS
    char L_RETURNFLAG;
    char L_LINESTATUS;
    double SUM_QTY;
    double SUM_BASE_PRICE;
    double SUM_DISC_PRICE;
    double SUM_CHARGE;
    double SUM_EXT_PRICE;
    double SUM_DISCOUNT;
    int COUNT_ORDER;

    double * src_QTY;
    double * src_EXT_PRICE;
    double * src_DISCOUNT;
    double * src_TAX;

public:
  
    count_aggregate_t() {
	SUM_QTY = 0.0;
	SUM_BASE_PRICE = 0.0;
	SUM_DISC_PRICE = 0.0;
	SUM_CHARGE = 0.0;
	SUM_EXT_PRICE = 0.0;
	SUM_DISCOUNT = 0.0;
	COUNT_ORDER = 0;
    }
  
    bool aggregate(tuple_t &, const tuple_t & src) {

	// read tuple
	src_QTY = (double *)(src.data + sizeof(int));
	src_EXT_PRICE = (double *)(src.data + sizeof(int) + sizeof(double));
	src_DISCOUNT = (double *)(src.data + sizeof(int) + 2*sizeof(double));
	src_TAX = (double *)(src.data + sizeof(int) + 3*sizeof(double));

	// update STATS
	COUNT_ORDER++;

	SUM_QTY += *src_QTY;
	SUM_BASE_PRICE += *src_EXT_PRICE;
	SUM_DISC_PRICE += *src_EXT_PRICE * (1 - *src_DISCOUNT);
	SUM_CHARGE += *src_EXT_PRICE * (1 - *src_DISCOUNT) * (1 + *src_TAX);
	SUM_EXT_PRICE += *src_EXT_PRICE;
	SUM_DISCOUNT += *src_DISCOUNT;
    
	if (COUNT_ORDER % 100 == 0) {
	    TRACE(TRACE_DEBUG, "%d\n", COUNT_ORDER);
	    fflush(stdout);
	}

	return false;
    }

    
    bool eof(tuple_t &dest) {
	
	int dest_offset = 0;
	memcpy(dest.data + dest_offset, &L_RETURNFLAG, sizeof(char));

	dest_offset += sizeof(char);
	memcpy(dest.data + dest_offset, &L_LINESTATUS, sizeof(char));

	dest_offset += sizeof(char);
	memcpy(dest.data + dest_offset, &SUM_QTY, sizeof(double));

	dest_offset += sizeof(double);
	memcpy(dest.data + dest_offset, &SUM_BASE_PRICE, sizeof(double));

	dest_offset += sizeof(double);
	memcpy(dest.data + dest_offset, &SUM_DISC_PRICE, sizeof(double));

	dest_offset += sizeof(double);
	memcpy(dest.data + dest_offset, &SUM_CHARGE, sizeof(double));

	dest_offset += sizeof(double);
	double AVG_QTY = SUM_QTY / COUNT_ORDER;
	memcpy(dest.data + dest_offset, &AVG_QTY, sizeof(double));

	dest_offset += sizeof(double);
	double AVG_PRICE = SUM_EXT_PRICE / COUNT_ORDER;
	memcpy(dest.data + dest_offset, &AVG_PRICE, sizeof(double));

	dest_offset += sizeof(double);
	double AVG_DISC = SUM_DISCOUNT / COUNT_ORDER;
	memcpy(dest.data + dest_offset, &AVG_DISC, sizeof(double));

	dest_offset += sizeof(double);
	memcpy(dest.data + dest_offset, &COUNT_ORDER, sizeof(int));

	return true;
    }
};

// END OF: Q6 AGG



/** @fn    : void * drive_stage(void *)
 *  @brief : Simulates a worker thread on the specified stage.
 *  @param : arg A stage_t* to work on.
 */

void *drive_stage(void *arg) {

    stage_container_t* sc = (stage_container_t*)arg;
    sc->run();
    
    return NULL;
}


/** @fn    : main
 *  @brief : TPC-H Q6
 */

int main() {

    thread_init();

    trace_current_setting = TRACE_ALWAYS;
    int THREAD_POOL_SIZE = 10;

    // creates a TSCAN stage
    stage_container_t* sc;

    sc = new stage_container_t("TSCAN_CONTAINER", new stage_factory<tscan_stage_t>);
    dispatcher_t::register_stage_container(tscan_packet_t::PACKET_TYPE, sc);
    tester_thread_t* tscan_thread = new tester_thread_t(drive_stage, sc, "TSCAN THREAD");
    if ( thread_create(NULL, tscan_thread) ) {
	TRACE(TRACE_ALWAYS, "thread_create failed\n");
	QPIPE_PANIC();
    }

    // creates a FDUMP stage
    sc = new stage_container_t("FDUMP_CONTAINER", new stage_factory<fdump_stage_t>);
    dispatcher_t::register_stage_container(fdump_packet_t::PACKET_TYPE, sc);
    for (int i = 0; i < THREAD_POOL_SIZE; i++) {
	tester_thread_t* fdump_thread = new tester_thread_t(drive_stage, sc, "FDUMP THREAD");
	if ( thread_create( NULL, fdump_thread ) ) {
	    TRACE(TRACE_ALWAYS, "thread_create failed\n");
	    QPIPE_PANIC();
	}
    }

    // creates a FSCAN stage    
    sc = new stage_container_t("FSCAN_CONTAINER", new stage_factory<fscan_stage_t>);
    dispatcher_t::register_stage_container(fscan_packet_t::PACKET_TYPE, sc);
    for (int i = 0; i < THREAD_POOL_SIZE; i++) {
	tester_thread_t* fscan_thread = new tester_thread_t(drive_stage, sc, "FSCAN THREAD");
	if ( thread_create( NULL, fscan_thread ) ) {
	    TRACE(TRACE_ALWAYS, "thread_create failed\n");
	    QPIPE_PANIC();
	}
    }

    // creates a MERGE stage    
    sc = new stage_container_t("MERGE_CONTAINER", new stage_factory<merge_stage_t>);
    dispatcher_t::register_stage_container(merge_packet_t::PACKET_TYPE, sc);
    for (int i = 0; i < THREAD_POOL_SIZE; i++) {
	tester_thread_t* merge_thread = new tester_thread_t(drive_stage, sc, "MERGE THREAD");
	if ( thread_create( NULL, merge_thread ) ) {
	    TRACE(TRACE_ALWAYS, "thread_create failed\n");
	    QPIPE_PANIC();
	}
    }

    // creates a SORT stage
    sc = new stage_container_t("SORT_CONTAINER", new stage_factory<sort_stage_t>);
    dispatcher_t::register_stage_container(sort_packet_t::PACKET_TYPE, sc);
    for (int i = 0; i < THREAD_POOL_SIZE; i++) {
	tester_thread_t* sort_thread = new tester_thread_t(drive_stage, sc, "SORT THREAD");
	if ( thread_create( NULL, sort_thread ) ) {
	    TRACE(TRACE_ALWAYS, "thread_create failed\n");
	    QPIPE_PANIC();
	}
    }

    // creates a AGG stage
    sc = new stage_container_t("AGG_CONTAINER", new stage_factory<aggregate_stage_t>);
    dispatcher_t::register_stage_container(aggregate_packet_t::PACKET_TYPE, sc);
    tester_thread_t* agg_thread = new tester_thread_t(drive_stage, sc, "AGG THREAD");
    if ( thread_create(NULL, agg_thread) ) {
	TRACE(TRACE_ALWAYS, "thread_create failed\n");
	QPIPE_PANIC();
    }

    // OPENS THE LINEITEM TABLE
    Db* tpch_lineitem = NULL;
    DbEnv* dbenv = NULL;

    try {
	// the tscan packet does not have an input buffer but a table

	// Create an environment object and initialize it for error
	// reporting.
	dbenv = new DbEnv(0);
	dbenv->set_errpfx("qpipe");
	
	// We want to specify the shared memory buffer pool cachesize,
	// but everything else is the default.
  
	//dbenv->set_cachesize(0, TPCH_BUFFER_POOL_SIZE, 0);
	if (dbenv->set_cachesize(TPCH_BUFFER_POOL_SIZE_GB, TPCH_BUFFER_POOL_SIZE_BYTES, 0)) {
	    TRACE(TRACE_ALWAYS, "*** Error while trying to set BUFFERPOOL size ***\n");
	}
	else {
	    TRACE(TRACE_ALWAYS, "*** BUFFERPOOL SIZE SET: %d GB + %d B ***\n", TPCH_BUFFER_POOL_SIZE_GB, TPCH_BUFFER_POOL_SIZE_BYTES);
	}

	// Databases are in a subdirectory.
	dbenv->set_data_dir(CONFIG_DATA_DIR);
  
	// set temporary directory
	dbenv->set_tmp_dir(TMP_DIR);

	// Open the environment with no transactional support.
	dbenv->open(DATABASE_HOME, DB_CREATE | DB_PRIVATE | DB_THREAD | DB_INIT_MPOOL, 0);

	tpch_lineitem = new Db(dbenv, 0);


	tpch_lineitem->set_bt_compare(tpch_lineitem_bt_compare_fcn);
	tpch_lineitem->open(NULL, TABLE_LINEITEM_NAME, NULL, DB_BTREE,
			    DB_RDONLY | DB_THREAD, 0644);

	TRACE(TRACE_ALWAYS, "Lineitem table opened...\n");
    }
    catch ( DbException &e) {
	TRACE(TRACE_ALWAYS, "DbException: %s\n", e.what());
    }
    catch ( std::exception &en) {
	TRACE(TRACE_ALWAYS, "std::exception\n");
    }
    

    for(int i=0; i < 10; i++) {
        stopwatch_t timer;
        
        // TSCAN PACKET
        // the output consists of 2 doubles
	tuple_buffer_t tscan_out_buffer(sizeof(int) + 4 * sizeof(double));
        tuple_filter_t *tscan_filter = new q1_tscan_filter_t();
        tscan_packet_t *q1_tscan_packet = new tscan_packet_t("q1 tscan",
                                                             &tscan_out_buffer,
                                                             tscan_filter,
                                                             NULL, /* no need for client_buffer */
                                                             tpch_lineitem);
   
	// SORT PACKET
	// the output is always a single int
	tuple_buffer_t sort_out_buffer(sizeof(int) + 4 * sizeof(double));
	tuple_filter_t* sort_filter = new tuple_filter_t(tscan_out_buffer.tuple_size);
	int_tuple_comparator_t *compare = new int_tuple_comparator_t;
	sort_packet_t* q1_sort_packet = new sort_packet_t("q1 sort",
							     &sort_out_buffer,
							     &sort_out_buffer,
							     &tscan_out_buffer,
							     sort_filter,
							     compare);

        // AGG PACKET CREATION
        // the output consists of 2 int
        tuple_buffer_t  agg_output_buffer(2*sizeof(double));
        tuple_filter_t* agg_filter = new tuple_filter_t(agg_output_buffer.tuple_size);
        count_aggregate_t*  q1_aggregator = new count_aggregate_t();
        aggregate_packet_t* q1_agg_packet = new aggregate_packet_t("q1 aggregate",
                                                                   &agg_output_buffer,
                                                                   agg_filter,
                                                                   &sort_out_buffer,
                                                                   q1_aggregator);


        // Dispatch packet
        dispatcher_t::dispatch_packet(q1_tscan_packet);
	dispatcher_t::dispatch_packet(q1_sort_packet);
        dispatcher_t::dispatch_packet(q1_agg_packet);
    
        tuple_t output;
        agg_output_buffer.init_buffer();

        double * r = NULL;
	
	TRACE(TRACE_ALWAYS, "*** Q1 ANSWER ...\n");
	TRACE(TRACE_ALWAYS, "*** SUM_QTY\tSUM_BASE\tSUM_DISC...\n");

        while(agg_output_buffer.get_tuple(output)) {
            r = (double*)(output.data + 2*sizeof(char));
            TRACE(TRACE_ALWAYS, "*** %lf\t%lf\t*\n", r[0], r[1], r[2]);
        }
        
        printf("Query executed in %lf ms\n", timer.time_ms());
    }
    try {    
	// closes file and environment
	TRACE(TRACE_DEBUG, "Closing Storage Manager...\n");

	// Close tables
	tpch_lineitem->close(0);
    
	// Close the handle.
	dbenv->close(0);
    }
    catch ( DbException &e) {
	TRACE(TRACE_ALWAYS, "DbException: %s\n", e.what());
    }
    catch ( std::exception &en) {
	TRACE(TRACE_ALWAYS, "std::exception\n");
    }

    return 0;
}
