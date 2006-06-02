// -*- mode:C++ c-basic-offset:4 -*-

#include "thread.h"
#include "tester_thread.h"
#include "stages/tscan.h"
#include "stages/aggregate.h"
#include "trace/trace.h"
#include "qpipe_panic.h"

using namespace qpipe;


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



/** @fn    : void * drive_stage(void *)
 *  @brief : Simulates a worker thread on the specified stage.
 *  @param : arg A stage_t* to work on.
 */

void *drive_stage(void *arg)
{
    stage_t *stage = (stage_t *)arg;

    while(1) {
        stage->process_next_packet();
    }

    return NULL;
}



/* Specific TBSCAN filter for this client */

/* @TODO: tscan_filter should record the first (rid) and when it meets it again
   it should signal that it finished the processing */


class q6_filter_t : public tuple_filter_t {

public:
    virtual bool select(const tuple_t & input) {
	// TODO: Should ask the Catalog
	double* d_discount = (double*)(input.data + 4*sizeof(int)+3*sizeof(double));

	/* all the lineitems with discount > 0.04 pass the filter */
        if (*d_discount > 0.04) {
	    TRACE(TRACE_DEBUG, "Passed Filtering:\t %.2f\n", *d_discount);
	    return (true);
	}

	return (false);
    }
    
    virtual void project(tuple_t &dest, const tuple_t &src) {

	memcpy(dest.data, src.data, dest.size);
    }

};



/* Count aggregator */

class count_aggregate_t : public tuple_aggregate_t {

private:
  int count;
    
public:
  
  count_aggregate_t() {
    count = 0;
  }
  
  bool aggregate(tuple_t &, const tuple_t &) {
    count++;
    return false;
  }

  bool eof(tuple_t &dest) {
    *(int*)dest.data = count;
    return true;
  }
};



/** @fn    : main
 *  @brief : Calls a table scan and outputs a specific projection
 */

int main() {

    thread_init();

    // creates a TSCAN stage
    tscan_stage_t *tscan_stage = new tscan_stage_t();
    tester_thread_t* tscan_thread = new tester_thread_t(drive_stage, tscan_stage, "TSCAN THREAD");
    if ( thread_create( NULL, tscan_thread ) ) {
	TRACE(TRACE_ALWAYS, "thread_create failed\n");
	QPIPE_PANIC();
    }


    // creates a AGG stage
    aggregate_stage_t *agg_stage = new aggregate_stage_t();
    tester_thread_t* aggregate_thread = new tester_thread_t(drive_stage, agg_stage, "AGGREGATE THREAD");
    if ( thread_create( NULL, aggregate_thread ) ) {
	TRACE(TRACE_ALWAYS, "thread_create failed\n");
	QPIPE_PANIC();
    }


    pthread_mutex_t mutex;
    pthread_mutex_init_wrapper(&mutex, NULL);

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
    

    
    // TSCAN PACKET CREATION
    // the tbscan output consists of 1 integer
    tuple_buffer_t q6_tscan_output_buffer(sizeof(int));
    tuple_filter_t *q6_tscan_filter = new q6_filter_t();
        
    tscan_packet_t *q6_tscan_packet = new tscan_packet_t("q6 tscan",
							 tpch_lineitem,
							 /* TODO: Should get that size from Catalog */
							 sizeof(tpch_lineitem_tuple),
							 &q6_tscan_output_buffer,
							 q6_tscan_filter);


    // AGG PACKET CREATION
    // the output is always a single int
    tuple_buffer_t  q6_agg_output_buffer(sizeof(int));
    tuple_filter_t* q6_agg_filter = new tuple_filter_t();
    count_aggregate_t*  q6_aggregator = new count_aggregate_t();
    
    aggregate_packet_t* q6_agg_packet = new aggregate_packet_t("test aggregate",
							       &q6_agg_output_buffer,
							       &q6_tscan_output_buffer,
							       q6_agg_filter,
							       q6_aggregator);
    
    // ENQUEUE PACKETS
    agg_stage->enqueue(q6_agg_packet);
    tscan_stage->enqueue(q6_tscan_packet);
    
    tuple_t output;
    q6_agg_output_buffer.init_buffer();
    q6_tscan_output_buffer.init_buffer();

    while(q6_agg_output_buffer.get_tuple(output))
      TRACE(TRACE_ALWAYS, "*** Q6 Count: %d ***\n", *(int*)output.data);

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


    pthread_mutex_destroy_wrapper(&mutex);
    return 0;
}
