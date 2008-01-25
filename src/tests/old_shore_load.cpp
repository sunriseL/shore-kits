/* -*- mode:C++; c-basic-offset:4 -*- */
// CC -m64 -xarch=ultraT1 -xs -g -I $SHORE_INCLUDE_DIR shore_tpcc_load.cpp -o shore_tpcc_load -L $SHORE_LIB_DIR -mt -lsm -lsthread -lfc -lcommon -lpthread

/** Original shore_tpcc_load.cpp */

#include "sm_vas.h"
#include "util/progress.h"
#include "util/c_str.h"
#include "workload/tpcc/tpcc_tbl_parsers.h"
#include <utility>

using namespace tpcc;

/* create an smthread based class for all sm-related work */
class smthread_user_t : public smthread_t {
    int	argc;
    char	**argv;
public:
    int	retval;
    
    smthread_user_t(int ac, char **av) 
	: smthread_t(t_regular, "smthread_user_t"),
	  argc(ac), argv(av), retval(0)
    {
    }
    void run();
};

#define MAX_FNAME_LEN = 31;
struct file_info_t {
    std::pair<int,int> _record_size;
    stid_t 	_table_id;
    rid_t 	_first_rid;
};

struct parse_thread : public smthread_t {
    c_str _fname; // file name
    ss_m* _ssm; // database handle
    vid_t _vid;
    file_info_t _info;
    sm_stats_info_t _stats;
    
public:
    parse_thread(c_str fname, ss_m* ssm, vid_t vid)
	: _fname(fname), _ssm(ssm), _vid(vid)
    {
	memset(&_stats, 0, sizeof(_stats));
    }
    virtual void run()=0;
    ~parse_thread() {}
};

template <class Parser>
struct parser_impl : public parse_thread {
    parser_impl(c_str fname, ss_m* ssm, vid_t vid)
	: parse_thread(fname, ssm, vid)
    {
    }
    void run();
};

struct test_parser : public parser_impl<parse_tpcc_ORDER> {
    test_parser(int tid, ss_m* ssm, vid_t vid)
	: parser_impl(c_str("tbl_tpcc/test-%02d.dat", tid), ssm, vid)
    {
    }
};

/* Runs a transaction, checking for deadlock and retrying
   automatically as need be. 'Transaction' must be a type whose
   function call operator takes no arguments and returns w_rc_t.
 */
template<class Transaction>
w_rc_t run_xct(ss_m* ssm, Transaction &t) {
    w_rc_t e;
    do {
	e = ssm->begin_xct();
	if(e)
	    break;
	e = t(ssm);
	if(e)
	    e = ssm->abort_xct();
	else
	    e = ssm->commit_xct();
    } while(e && e.err_num() == smlevel_0::eDEADLOCK);
    return e;
}
pthread_mutex_t vol_mutex = PTHREAD_MUTEX_INITIALIZER;

template<class Parser>
struct create_volume_xct {
    vid_t &_vid;
    char const* _table_name;
    file_info_t &_info;
    size_t _bytes;

    create_volume_xct(vid_t &vid, char const* tname, file_info_t &info, size_t bytes)
	: _vid(vid), _table_name(tname), _info(info), _bytes(bytes)
    {
    }

    w_rc_t operator()(ss_m* ssm) {
	CRITICAL_SECTION(cs, vol_mutex);
	stid_t root_iid;
	vec_t table_name(_table_name, strlen(_table_name));
	unsigned size = sizeof(_info);
	vec_t table_info(&_info, size);
	bool found;
	W_DO(ss_m::vol_root_index(_vid, root_iid));
	W_DO(ss_m::find_assoc(root_iid, table_name, &_info, size, found));
	if(found) {
	    cout << "Removing previous instance of " << _table_name << endl;
	    W_DO(ss_m::destroy_file(_info._table_id));
	    W_DO(ss_m::destroy_assoc(root_iid, table_name, table_info));
	}
	// create the file and register it with the root index
	cout << "Creating table ``" << _table_name
	     << "'' with " << _bytes << " bytes per record" << endl;
	W_DO(ssm->create_file(_vid, _info._table_id, smlevel_3::t_regular));
	W_DO(ss_m::vol_root_index(_vid, root_iid));
	W_DO(ss_m::create_assoc(root_iid, table_name, table_info));
	return RCOK;
    }
};

template <class Parser>
void parser_impl<Parser>::run() {
    FILE* fd = fopen(_fname.data(), "r");
    if(fd == NULL) {
        TRACE(TRACE_ALWAYS, "fopen() failed on %s\n", _fname.data());
	// TOOD: return an error code or something
	return;
    }

    unsigned long progress = 0;
    char linebuffer[MAX_LINE_LENGTH];
    Parser parser;
    typedef typename Parser::record_t record_t;
    static size_t const ksize = sizeof(record_t::first_type);
    static size_t const bsize = parser.has_body()? sizeof(record_t::second_type) : 0;

    // blow away the previous file, if any
    file_info_t info;
    create_volume_xct<Parser> cvxct(_vid, _fname.data(), info, ksize+bsize);
    W_COERCE(run_xct(_ssm, cvxct));
     
    sm_stats_info_t stats;
    memset(&stats, 0, sizeof(stats));
    sm_stats_info_t* dummy; // needed to keep xct commit from deleting our stats...
    
    W_COERCE(_ssm->begin_xct());
    
    // insert records one by one...
    record_t record;
    vec_t head(&record.first, ksize);
    vec_t body(&record.second, bsize);
    rid_t rid;
    bool first = true;
    static int const INTERVAL = 100000;
    int mark = INTERVAL;
    int i;
    
    for(i=0; fgets(linebuffer, MAX_LINE_LENGTH, fd); i++) {
	record = parser.parse_row(linebuffer);
	W_COERCE(_ssm->create_rec(info._table_id, head, bsize, body, rid));
	if(first)
	    info._first_rid = rid;
	
	first = false;
	progress_update(&progress);
	if(i >= mark) {
	    W_COERCE(_ssm->commit_xct(dummy));
	    W_COERCE(_ssm->begin_xct());
	    mark += INTERVAL;
	}	    
    }
    
    // done!
    W_COERCE(_ssm->commit_xct(dummy));
    info._record_size = std::make_pair(ksize, bsize);
    progress_done(parser.table_name());
    _info = info;
    cout << "Successfully loaded " << i << " records" << endl;
    if ( fclose(fd) ) {
	TRACE(TRACE_ALWAYS, "fclose() failed on %s\n", _fname.data());
	// TOOD: return an error code or something
	return;
    }

}


void
usage(option_group_t& options)
{
    cerr << "Valid options are: " << endl;
    options.print_usage(true, cerr);
}

void smthread_user_t::run() {
    option_group_t options(1);

    cout << "Configuring Shore..." << endl;
    
    // have the SSM add its options to the group
    W_COERCE(ss_m::setup_options(&options));

    // initialize the options we care about..
    static char const* opts[] = {
	"fake-command-line",
	"-sm_bufpoolsize", "102400", // in kB
	//	"-sm_logging", "no", // temporary
	"-sm_logdir", "log",
	"-sm_logsize", "102400", // in kB
	"-sm_logbufsize", "10240", // in kB
	"-sm_diskrw", "/export/home/ipandis/DEV/shore-lomond/installed/bin/diskrw",
	"-sm_errlog", "info", // one of {none emerg fatal alert internal error warning info debug}
    };
    w_ostrstream err;
    int opts_count = sizeof(opts)/sizeof(char const*); // clobbered by the parser
    w_rc_t rc = options.parse_command_line(opts, opts_count, 2, &err);
    err << ends;
    if(rc) {
	cerr << "Error configuring Shore: " << endl;
	cerr << "\t" << w_error_t::error_string(rc.err_num()) << endl;
	cerr << "\t" << err.c_str() << endl;
	usage(options);
    }

    // verify 
    w_reset_strstream(err);
    rc = options.check_required(&err);
    if (rc) {
	cerr << "These required options are not set:" << endl;
	cerr << err.c_str() << endl;
	usage(options);
    }

    cout << "Starting Shore..." << endl;
    guard<ss_m> ssm = new ss_m();

    // format? and mount the database...
    // TODO: make these command-line options!
    bool clobber = true;
    char const* device = "tbl_tpcc/shore";
    int quota = 100*1024; // 100 MB
    if(clobber) {
	cout << "Formatting a new device ``" << device
	     << "'' with a " << quota << "kB quota" << endl;

	// create and mount device
	// http://www.cs.wisc.edu/shore/1.0/man/device.ssm.html
	W_COERCE(ssm->format_dev(device, quota, true));
    }

    // mount it...
    unsigned vol_cnt;
    devid_t devid;
    W_COERCE(ssm->mount_dev(device, vol_cnt, devid));
    
    lvid_t lvid; // this is a "crutch for those... using the physical ID... interface"
    vid_t vid(1);
    if(clobber) {
	// create volume (only one per device supported, so this is kind of silly)
	// see http://www.cs.wisc.edu/shore/1.0/man/volume.ssm.html
	W_COERCE(ssm->generate_new_lvid(lvid));
	W_COERCE(ssm->create_vol(device, lvid, quota, false, vid));
    }
    else {
	lvid_t* lvid_list;
	unsigned lvid_cnt;
	W_COERCE(ssm->list_volumes(device, lvid_list, lvid_cnt));
	if (lvid_cnt == 0) {
	    cerr << "Error, device has no volumes" << endl;
	    exit();
	}	
	lvid = lvid_list[0];
	delete [] lvid_list;
    }

    static int const THREADS = 1;//8;
    guard<parse_thread> threads[THREADS];
    for(int i=0; i < THREADS; i++)
	threads[i] = new test_parser(i, ssm.get(), vid);
    for(int i=0; i < THREADS; i++)
	threads[i]->fork();

    sm_stats_info_t stats;
    memset(&stats, 0, sizeof(stats));
    for(int i=0; i < THREADS; i++) {
	threads[i]->join();
	//stats += threads[i]->_stats;
    }
    ss_m::gather_stats(stats, false);
    //    cout << stats << endl;   

    // done!
    cout << "Shutting down Shore..." << endl;
}

int main(int argc, char* argv[]) {
    smthread_user_t *smtu = new smthread_user_t(argc, argv);
    if (!smtu)
	W_FATAL(fcOUTOFMEMORY);

    w_rc_t e = smtu->fork();
    if(e) {
	cerr << "error forking main thread: " << e <<endl;
	return 1;
    }
    e = smtu->join();
    if(e) {
	cerr << "error joining main thread: " << e <<endl;
	return 1;
    }

    int	rv = smtu->retval;
    delete smtu;

    return rv;
}