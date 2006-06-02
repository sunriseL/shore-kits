// -*- mode:C++ c-basic-offset:4 -*-
#include "stages/tscan.h"
#include "trace/trace.h"

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


/* Specific filter for this client */

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




/** @fn    : main
 *  @brief : Calls a table scan and outputs a specific projection
 */

int main() {

    thread_init();

    tscan_stage_t *stage = new tscan_stage_t();

    pthread_t stage_thread;
    pthread_mutex_t mutex;

    pthread_mutex_init_wrapper(&mutex, NULL);
    
    pthread_create(&stage_thread, NULL, &drive_stage, stage);

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
    
    // the output consists of 3 integers
    tuple_buffer_t output_buffer(3*sizeof(int));
    tuple_filter_t *filter = new q6_filter_t();
    tscan_packet_t *packet = new tscan_packet_t("test tscan",
						tpch_lineitem,
						/* TODO: Should get that size from Catalog */
						sizeof(tpch_lineitem_tuple),
						&output_buffer,
						filter);
    
    stage->enqueue(packet);
    
    tuple_t output;
    output_buffer.init_buffer();

    int * d = NULL;

    while(output_buffer.get_tuple(output)) {
	d = (int*)output.data;
        TRACE(TRACE_ALWAYS, "Read ID: %d - %d - %d\n", d[0], d[1], d[2]);
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


    pthread_mutex_destroy_wrapper(&mutex);
    return 0;
}
