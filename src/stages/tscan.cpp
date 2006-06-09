/* -*- mode:C++; c-basic-offset:4 -*- */

#include "stages/tscan.h"
#include "qpipe_panic.h"
#include "utils.h"



const char* tscan_packet_t::PACKET_TYPE = "TSCAN";



const char* tscan_stage_t::DEFAULT_STAGE_NAME = "TSCAN_STAGE";



/**
 * @brief A cursor guard that automatically closes the cursor when it
 * goes out of scope.
 */
struct cursor_guard_t {
    Db*  _db;
    Dbc* _cursor;

    cursor_guard_t(Db* db, Dbc* cursor)
        : _db(db), _cursor(cursor)
    {
    }
    
    ~cursor_guard_t() {
        int ret = _cursor->close();
        if(ret)
            _db->err(ret, "_cursor->close() failed: ");
    }
};



/**
 *  @brief Read the specified table.
 *
 *  @return 0 on success. Non-zero on unrecoverable error. The stage
 *  should terminate all queries it is processing.
 */

int tscan_stage_t::process_packet() {

    adaptor_t* adaptor = _adaptor;
    tscan_packet_t* packet = (tscan_packet_t*)adaptor->get_packet();

    Dbc* dbcp;
    Db*  db = packet->_db;
    

    // open a cursor to the table
    int ret = db->cursor(NULL, &dbcp, 0);
    if (ret) {
        db->err(ret, "db->cursor() failed: ");
	TRACE(TRACE_ALWAYS, "db->cursor() failed\n");
	return -1;
    }
    cursor_guard_t cursor_guard(db, dbcp);
    

    // BerkeleyDB cannot read into page_t's. Allocate a large blob and
    // do bulk reading. The blob must be aligned for int accesses and a
    // multiple of 1024 bytes long.
    int bufsize = 1024 * (4096/sizeof(int));
    array_guard_t<int> buffer = new int[bufsize];

    
    // initiates and sets the key and data variables for the bulk reading
    Dbt bulk_key, bulk_data;
    memset(&bulk_key,  0, sizeof(bulk_key));
    memset(&bulk_data, 0, sizeof(bulk_data));
    bulk_data.set_data(buffer);
    bulk_data.set_ulen(bufsize);
    bulk_data.set_flags(DB_DBT_USERMEM);


    int bulk_reads =0;

    for (int bulk_read_index = 0; ; bulk_read_index++) {

	TRACE(TRACE_ALWAYS, "%d\n", ++bulk_reads);

	int err = dbcp->get(&bulk_key, &bulk_data, DB_MULTIPLE_KEY | DB_NEXT);
	if (err) {
	    if (err != DB_NOTFOUND) {
		db->err(err, "dbcp->get() failed: ");
		TRACE(TRACE_ALWAYS, "dbcp->get failed\n");
		return -1;
	    }
	    
	    // done reading table
	    return 0;
	}


        // iterate over the blob we read and output the individual
        // tuples
        Dbt key, data;
        DbMultipleKeyDataIterator it = bulk_data;
	for (int tuple_index = 0; it.next(key, data); tuple_index++) {

	    //    TRACE(TRACE_DEBUG, "Reading tuple %d in bulk read %d\n",
	    //  tuple_index,
	    //  bulk_read_index);

	    tuple_t current_tuple((char*)data.get_data(), packet->output_buffer->tuple_size);

	    stage_t::adaptor_t::output_t output_ret = adaptor->output(current_tuple);

	    switch (output_ret) {
	    case stage_t::adaptor_t::OUTPUT_RETURN_CONTINUE:
		continue;
	    case stage_t::adaptor_t::OUTPUT_RETURN_STOP:
		return 0;
	    case stage_t::adaptor_t::OUTPUT_RETURN_ERROR:
		return -1;
	    default:
		TRACE(TRACE_ALWAYS, "adaptor->output() return unrecognized value %d\n",
		      output_ret);
		QPIPE_PANIC();
	    }
        }
    }

    
    // control never reaches here
    TRACE(TRACE_ALWAYS, "Should not be here!\n");
    QPIPE_PANIC();
}
