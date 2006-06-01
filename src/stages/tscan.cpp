/* -*- mode:C++; c-basic-offset:4 -*- */

#include "stages/tscan.h"
#include "qpipe_panic.h"



void tscan_packet_t::terminate() {
    // do nothing -- no state to clean up
}


/**
 * @brief A cursor guard that automatically closes the cursor when it
 * goes out of scope.
 */
struct cursor_guard_t {
    Db *_db;
    Dbc *_cursor;
    cursor_guard_t(Db *db, Dbc *cursor)
        : _db(db), _cursor(cursor)
    {
    }
    
    ~cursor_guard_t() {
        int tmp_ret = _cursor->close();
        if(tmp_ret) {
            _db->err(tmp_ret, "ERROR closing cursor for fscan: ");
            QPIPE_PANIC();
        }
    }
};



/** @fn    : int process_packet(packet_t *)
 *  @brief : The (virtual) function that does the actual processing,
 *           for the table scan stage.
 */

int tscan_stage_t::process_packet(packet_t *p) {

    tscan_packet_t *packet = (tscan_packet_t *)p;

    Dbc *dbcp;
    Db *db = packet->input_table;

    // try to open a cursor (not thread safe)
    int tmp_ret = db->cursor(packet->xact_id, &dbcp, 0);
    if(tmp_ret != 0) {
        db->err(tmp_ret, "ERROR opening cursor for tscan: ");
        QPIPE_PANIC();
    }
    
    // make sure the cursor gets closed properly
    cursor_guard_t cursor_guard(db, dbcp);
    
    // initialize data buffer for bulk reading. Must be aligned for
    // int accesses and a multiple of 1024 bytes long.
    // TODO: use actual pages somehow
    int bufsize = 10*4096/sizeof(int);
    int* buffer = new int[bufsize];
    Dbt bulk_key, bulk_data;
    memset(&bulk_key, 0, sizeof(bulk_key));
    memset(&bulk_data, 0, sizeof(bulk_data));
    bulk_data.set_data(buffer);
    bulk_data.set_ulen(bufsize);
    bulk_data.set_flags(DB_DBT_USERMEM);

    while(1) {
        // request a group of tuples
        int tmp_ret = dbcp->get(&bulk_key, &bulk_data,
                                DB_MULTIPLE_KEY | DB_NEXT);
        if(tmp_ret != 0)
            break;

        // iterator over the group
        Dbt key, data;
        DbMultipleKeyDataIterator it = bulk_data;
        while(it.next(key, data)) {
            if(!output(packet, data)) 
                return 1;
        }
    }

    // test for read errors
    if(tmp_ret != DB_NOTFOUND) {
        db->err(tmp_ret, "ERROR accessing cursor for tscan: ");
        QPIPE_PANIC();
    }

    return 0;
}
