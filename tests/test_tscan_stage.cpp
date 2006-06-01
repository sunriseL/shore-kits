// -*- mode:C++ c-basic-offset:4 -*-
#include "stages/tscan.h"
#include "trace/trace.h"

using namespace qpipe;


/* Declaration of some constants */

# define DATABASE_HOME	 "."
# define CONFIG_DATA_DIR "./database"
# define TMP_DIR "./tmp"

# define TABLE_LINEITEM_NAME "LINEITEM"
# define TABLE_LINEITEM_ID   "TBL_LITEM"

/* Set Bufferpool equal to 450 MB -- Maximum is 4GB in 32-bit platforms */
size_t TPCH_BUFFER_POOL_SIZE_GB = 0; /* 0 GB */
size_t TPCH_BUFFER_POOL_SIZE_BYTES = 450 * 1024 * 1024; /* 450 MB */


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

class q6_filter_t : public tuple_filter_t {
public:
    virtual bool select(const tuple_t & input) {
	// TODO: Should ask the Catalog
	// naive filtering 
        if ((int)(*(int*)input.data)) {
	    return (true);
	}
	
	return (false);
    }

    
    virtual void project(tuple_t &dest, const tuple_t &src) {
	// copies only the first three integers of the tuple
	dest.assign(src);
    }

};




/** @fn    : main
 *  @brief : Calls a table scan and outputs a specific projection
 */

int main() {

    tscan_stage_t *stage = new tscan_stage_t();

    pthread_t stage_thread;
    pthread_t source_thread;
    pthread_mutex_t mutex;

    pthread_mutex_init_wrapper(&mutex, NULL);
    
    pthread_create(&stage_thread, NULL, &drive_stage, stage);

    // the tscan packet does not have an input buffer
    // but a table
    Db* tpch_lineitem = NULL;

    //
    // Create an environment object and initialize it for error
    // reporting.
    //
    DbEnv* dbenv = new DbEnv(0);
    dbenv->set_errpfx("qpipe");
	
    //
    // We want to specify the shared memory buffer pool cachesize,
    // but everything else is the default.
    //

  
    //dbenv->set_cachesize(0, TPCH_BUFFER_POOL_SIZE, 0);
    if (dbenv->set_cachesize(TPCH_BUFFER_POOL_SIZE_GB, TPCH_BUFFER_POOL_SIZE_BYTES, 0)) {
	fprintf(stdout, "*** Error while trying to set BUFFERPOOL size ***\n");
    }
    else {
	fprintf(stdout, "*** BUFFERPOOL SIZE SET: %d GB + %d B ***\n", TPCH_BUFFER_POOL_SIZE_GB, TPCH_BUFFER_POOL_SIZE_BYTES);
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

    TRACE(TRACE_ALWAYS, "Lineitem table opened...");

    
    // the output is always a single int
    tuple_buffer_t output_buffer(sizeof(int));

    tuple_filter_t *filter = new q6_filter_t();
        
    tscan_packet_t *packet = new tscan_packet_t("test tscan",
						&output_buffer,
						filter,
						tpch_lineitem);
    
    stage->enqueue(packet);
    
    tuple_t output;
    output_buffer.init_buffer();
    while(output_buffer.get_tuple(output))
        printf("Count: %d\n", *(int*)output.data);


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
