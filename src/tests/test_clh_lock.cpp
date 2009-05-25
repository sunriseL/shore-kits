// -*- mode:c++; c-basic-offset:4 -*-
#include "util/clh_lock.h"
#include <cassert>
#include <pthread.h>
#include <stdlib.h>
#include "util/stopwatch.h"
#include <stdio.h>
#include "util/clh_rwlock.h"
#include <algorithm>

#include "util/atomic_ops.h"

#include "util/mcs_lock.h"
#include "util/mcs_rwlock.h"


// only works on sparc...
#include "sys/time.h"

namespace tatas {
    static int const READER = 0x2;
    static int const WRITER = 0x1;
    struct rwlock {
	unsigned _state;
	rwlock() : _state(0) { }
	void tatas(unsigned delta) {
	    static int const BACKOFF_BASE = 1;
	    static int const BACKOFF_FACTOR = 2;
	    static int const BACKOFF_MAX = 1 << 16;
	    int volatile b = BACKOFF_BASE;
	    while(1) {
		unsigned old_state = _state;
		unsigned readers_only = old_state & -READER;
		if(old_state == readers_only &&
#ifdef __SUNPRO_CC
		   atomic_cas_32(&_state, readers_only, readers_only+delta) == readers_only
#else
		   __sync_bool_compare_and_swap(&_state, readers_only, readers_only+delta)
#endif
		   )
		    break;
		for(int i=b; i; --i);
		b = std::min(b*2, BACKOFF_MAX);
	    }
	}
	void acquire_read() {
	    tatas(READER);
#ifdef __sparcv9
	    membar_enter();
#else
	    __sync_synchronize();
#endif
	    //	    assert(!(_state & ~0xff)); // writers may be pending, 1..127 readers
	}
	void release_read() {
	    //	    assert(!(_state & WRITER));
	    //	    unsigned new_state = atomic_add_32_nv(&_state, -READER);
#ifdef __SUNPRO_CC
	    membar_exit();
	    atomic_add_32(&_state, -READER);
#else
	    __sync_fetch_and_add(&_state, -READER);
#endif
	    //	    assert(!(new_state & ~0xff)); // 0..126 readers, writers may be pending
	}
	void acquire_write() {
	    static int const BACKOFF_BASE = 1;
	    static int const BACKOFF_FACTOR = 2;
	    static int const BACKOFF_MAX = 1 << 16;

	    // stake my claim
	    tatas(WRITER);

	    // wait for readers to finish
	    int volatile b = BACKOFF_BASE;
	    while(_state != WRITER) {
		for(int i=b; i; --i);
		b = std::min(b*BACKOFF_FACTOR, BACKOFF_MAX);
	    }
#ifdef __SUNPRO_CC
	    membar_enter();
#else
	    __sync_synchronize();
#endif
	    //	    assert(_state == WRITER);
	}
	void release_write() {
#ifdef __SUNPRO_CC
	    membar_exit();
	    //	    assert(_state == WRITER);
	    _state = 0;
#else
	    __sync_lock_release(&_state);
#endif
	    
	}
    };

} // EOF: namespace

static long const ONE_WRITER = 0x1;
static long const ONE_READER = 0x100;
static long const WRITE_MASK = 0xff;
static long const READ_MASK = 0xff00;
struct fast_rwlock {
    struct qnode;
    typedef qnode volatile* qnode_ptr;
    struct qnode {
	qnode_ptr _pred;
	bool volatile _waiting;
	qnode() : _pred(NULL), _waiting(false) { }
    };


    qnode_ptr volatile _tail;
    uint64_t volatile _state;

    fast_rwlock() : _tail(new qnode), _state(0) { }
    ~fast_rwlock() { delete _tail; }

    void spin_queue(qnode_ptr pred) {
	stopwatch_t timer;
	long start = timer.now();
	long now;
	while(pred->_waiting) {
	    //      assert((now=timer.now()) < start + 10000);
	}
	#ifdef __SUNPRO_CC
	membar_consumer();
	#else
	__sync_synchronize();
	#endif
    }
    void spin_state(long mask, long expected) {
	stopwatch_t timer;
	long start = timer.now();
	long now;
	while((_state & mask) != expected) {
	    //=     assert((now=timer.now()) < start + 10000);
	}
	#ifdef __SUNPRO_CC
	membar_enter(); // this is the last thing my caller does...
	#else
	__sync_synchronize();
	#endif
    }
    qnode_ptr acquire_write(qnode_ptr me) {
	//assert(_state < 0x10000);
	static int const WRITE_CAS_ATTEMPTS = 3;

	// first, try to CAS directly
	for(int i=0; i < WRITE_CAS_ATTEMPTS; i++) {
	    if(
	       #ifdef __SUNPRO_CC
	       _state == 0 && atomic_cas_64(&_state, 0, ONE_WRITER) == 0
	       #else
	       __sync_bool_compare_and_swap(&_state, 0, ONE_WRITER)
	       #endif
	       ) {
		//assert(_state < 0x10000);
		me->_pred = NULL; // indicates the CAS succeeded
		#ifdef __SUNPRO_CC
		membar_enter();
		#endif
		return me;
	    }
	}

	// enqueue
	me->_waiting = true;
	#ifdef __SUNPRO_CC
	membar_producer(); // finish updates to i *before* it hits the queue
	qnode_ptr pred = (qnode_ptr) atomic_swap_ptr(&_tail, (void*) me);
	#else
	__sync_synchronize();
	qnode_ptr pred = __sync_lock_test_and_set(&_tail, me);
	#endif

	// wait our turn
	spin_queue(me->_pred = pred);

	/* stake our claim so readers stop grabbing the lock. At most
	   one other writer might be ahead of us at this point -- the
	   one whose CAS succeeded between my last reader calling
	   release() and me finishing this atomic add. All other
	   potential writers will be in the queue behind me, and
	   haven't staked their claims yet.
	*/
	#ifdef __SUNPRO_CC
	atomic_add_64(&_state, ONE_WRITER);
	#else
	__sync_fetch_and_add(&_state, ONE_WRITER);
	#endif
	//assert(_state < 0x10000);

	// now spin until we have the lock
	spin_state(-1, ONE_WRITER);

	// no need to pass the baton to our successor quite yet...
	return me;
    }

    qnode_ptr release_write(qnode_ptr me) {
	//assert(_state < 0x10000);
	qnode_ptr pred = me->_pred;

	#ifdef __SUNPRO_CC
	membar_exit();
	atomic_add_64(&_state, -ONE_WRITER);
	#else
	__sync_fetch_and_add(&_state, -ONE_WRITER);
	#endif
	//assert(_state < 0x10000);
	if(pred) {
	    /* we are in the queue - pass the baton to our successor
	       (they may very well block if there's another writer
	       around, but we have to wake them up any way or risk
	       deadlock.
	    */
	    me->_waiting = false;
	    me = pred;
	}
	return me;
    }

    qnode_ptr acquire_read(qnode_ptr me) {
	//assert(_state < 0x10000);
	static int const READ_CAS_ATTEMPTS = 3;

	// first, try to CAS directly
	for(int i=0; i < READ_CAS_ATTEMPTS; i++) {
	    // fail immediately if there's a writer
	    long old_state = _state & READ_MASK;
	    if(
	       #ifdef __SUNPRO_CC
	       _state == old_state &&  atomic_cas_64(&_state, old_state, old_state+ONE_READER) == old_state
	       #else
	       __sync_bool_compare_and_swap(&_state, old_state, old_state+ONE_READER)
	       #endif
	       ) {
		//assert(_state < 0x10000);
		me->_pred = NULL; // indicates the CAS succeeded
		#ifdef __SUNPRO_CC
		membar_enter();
		#endif
		return me;
	    }
	}

	// enqueue
	me->_waiting = true;
	#ifdef __SUNPRO_CC
	membar_producer(); // finish updates to i *before* it hits the queue
	qnode_ptr pred = (qnode_ptr) atomic_swap_ptr(&_tail, (void*) me);
	#else
	__sync_synchronize();
	qnode_ptr pred = __sync_lock_test_and_set(&_tail, me);
	#endif

	// wait our turn
	spin_queue(me->_pred = pred);

	// stake our claim
	#ifdef __SUNPRO_CC
	atomic_add_64(&_state, ONE_READER);
	#else
	__sync_fetch_and_add(&_state, ONE_READER);
	#endif
	//assert(_state < 0x10000);

	// now spin until we have the lock
	spin_state(WRITE_MASK, 0);

	// pass the baton to our successor
	me->_waiting = false;
	pred->_pred = pred; // anything but NULL
	return pred;
    }

    qnode_ptr release_read(qnode_ptr me) {
	//assert(_state < 0x10000);
	// we already passed the baton
	#ifdef __SUNPRO_CC
	membar_exit();
	atomic_add_64(&_state, -ONE_READER);
	#else
	__sync_fetch_and_add(&_state, -ONE_READER);
	#endif
	//assert(_state < 0x10000);
	return me;
    }
};



static int const THREADS = 32;
static long const COUNT = 1l << 20;

#include <unistd.h>
volatile bool ready;
long local_count;
extern "C" void* run(void* arg) {
    clh_lock::thread_init_manager();
    clh_rwlock::thread_init_manager();
    
    while(!ready);
    stopwatch_t timer;
#if 1
    ((void (*)()) arg)();
    //    usleep(local_count*10);
#else
    unsigned dummy = 0;
    for(int i=0; i < 0*local_count; i++) {
	membar_producer();
	atomic_inc_32(&dummy);
    }
#endif
    union {
	double d;
	void* vptr;
    } u = {timer.time()};

     clh_lock::thread_destroy_manager();
     clh_rwlock::thread_destroy_manager();
     return u.vptr;
}

static long const NCS_LINES = 0; // 1usec
static int const CS_LINES = 10;
struct cache_line {
    int data;
    int filler[64/sizeof(int)-1]; // 64-byte lines
};
void ncs(long lines = NCS_LINES) {

    static int const MAX_LINES = 128;
    static __thread cache_line volatile local_data[MAX_LINES];

    // simulate an expensive data operation
    volatile long sum = 0;
    for(int i=0; i < lines; i++)
	sum += local_data[i].data;
}
void cs(long lines=CS_LINES) {

    static int const MAX_LINES = 128;
    static cache_line volatile shared_data[MAX_LINES];

    // simulate an expensive read (safe for R-locks)
    volatile long sum = 0;
    for(int i=0; i < lines; i++) {
	sum += shared_data[i].data;
    }
}

typedef std::pair<char const*, void (*)()> tfunction;
typedef std::vector<tfunction> function_list;
static function_list test_functions;

struct register_function {
    register_function(tfunction f) {
	test_functions.push_back(f);
    }
};
#define REGISTER_FUNCTION(name) \
    static register_function register_##name(std::make_pair(#name, &name))

#define TEST_RWLOCK(name, init, acquire_read, acquire_write, release_read, release_write) \
    void test_ ## name ## _rlock() {						\
	init;								\
	for(long i=0; i < local_count; i++) {				\
	    ncs();							\
	    acquire_read;						\
	    cs();							\
	    release_read;						\
	}								\
    }									\
    void test_ ## name ## _wlock() {						\
	init;								\
	for(long i=0; i < local_count; i++) {				\
	    ncs();							\
	    acquire_write;						\
	    cs();							\
	    release_write;						\
	}								\
    }									\
    void test_ ## name ## _rwlock() {						\
	init;								\
	static __thread int dummy = 0;					\
	long offset = ((long) &dummy >> 3) & 31;			\
	for(long i=0; i < local_count; i++) {				\
	    if(((i + offset) & 3) == 0) {				\
		ncs();							\
		acquire_write;						\
		cs();							\
		release_write;						\
	    }								\
	    else {							\
		ncs();							\
		acquire_read;						\
		cs();							\
		release_read;						\
	    }								\
	}								\
    }									\
    REGISTER_FUNCTION(test_ ## name ## _rlock);				\
    REGISTER_FUNCTION(test_ ## name ## _wlock);				\
    REGISTER_FUNCTION(test_ ## name ## _rwlock)

#define TEST_LOCK(name, init, acquire, release) \
    void test_ ## name ## _lock() {		\
	init;					\
	for(long i=0; i < local_count; i++) {	\
	    ncs();				\
	    acquire;				\
	    cs();			    	\
	    release;				\
	}					\
    }						\
    REGISTER_FUNCTION(test_ ## name ## _lock)
    
clh_lock global_lock;
pthread_mutex_t global_plock = PTHREAD_MUTEX_INITIALIZER;
pthread_rwlock_t global_rwlock = PTHREAD_RWLOCK_INITIALIZER;
mcs_lock global_mcs_lock;

#if 1
TEST_LOCK(pthread, sizeof(int), pthread_mutex_lock(&global_plock), pthread_mutex_unlock(&global_plock));
#endif

#if 1
TEST_LOCK(clh_manual, clh_lock::dead_handle h = clh_lock::create_handle(),
	  clh_lock::live_handle lh = global_lock.acquire(h), h = global_lock.release(lh));
//TEST_LOCK(clh_semiauto, clh_lock::Manager *m = clh_lock::_manager, global_lock.acquire(m), global_lock.release(m));
TEST_LOCK(clh_auto, sizeof(int), global_lock.acquire(), global_lock.release());
#endif

#if 1
TEST_LOCK(mcs, mcs_lock::qnode me, global_mcs_lock.acquire(me), global_mcs_lock.release(me));
#endif  

#if 0 // way too slow!
TEST_RWLOCK(pthread, sizeof(int),
	    pthread_rwlock_rdlock(&global_rwlock), pthread_rwlock_wrlock(&global_rwlock),
	    pthread_rwlock_unlock(&global_rwlock), pthread_rwlock_unlock(&global_rwlock));
#endif
#if 0
clh_rwlock global_clh_rwlock;
TEST_RWLOCK(clh_manual, clh_rwlock::dead_handle h = clh_rwlock::create_handle(),
	    clh_rwlock::live_handle l = global_clh_rwlock.acquire_read(h),
	    clh_rwlock::live_handle l = global_clh_rwlock.acquire_write(h),
	    h = global_clh_rwlock.release(l), h = global_clh_rwlock.release(l));
TEST_RWLOCK(clh_auto, sizeof(int),
	    global_clh_rwlock.acquire_read(), global_clh_rwlock.acquire_write(),
	    global_clh_rwlock.release(), global_clh_rwlock.release());
#endif
#if 0
fast_rwlock global_fast_rwlock;
TEST_RWLOCK(fast_manual, fast_rwlock::qnode_ptr me = new fast_rwlock::qnode,
	    me = global_fast_rwlock.acquire_read(me), me = global_fast_rwlock.acquire_write(me),
	    me = global_fast_rwlock.release_read(me), me = global_fast_rwlock.release_write(me));
#endif
#if 0
mcs_rwlock global_mcs_rwlock;
TEST_RWLOCK(mcs, mcs_rwlock::qnode me,
	    global_mcs_rwlock.acquire_read(&me), global_mcs_rwlock.acquire_write(&me),
	    global_mcs_rwlock.release_read(&me), global_mcs_rwlock.release_write(&me));
#endif

#if 0 // broken
tatas::rwlock global_tatas_rwlock;
TEST_RWLOCK(tatas, sizeof(int),
	    global_tatas_rwlock.acquire_read(), global_tatas_rwlock.acquire_write(),
	    global_tatas_rwlock.release_read(), global_tatas_rwlock.release_write());
#endif

#define CHOOSE_TEST(func)				\
    void *test_func = (void*) &func;			\
    char const* test_name = #func

struct run_list {
    double times[THREADS];
};

#include <string.h>
#include <unistd.h>
int main() {
    pthread_t tids[THREADS];
    long thread_delta = 1;
    
    std::vector<run_list> runs; // one per type...
    static int const SAMPLE_SIZE = 3;
    char const progress[] = { '|', '/', '-', '\\' };
    char const* dots[] = { "", ".", ".." };
    // ====================================================================== 
    //    CHOOSE_TEST(test_fast_rlock);
    // ======================================================================
    fprintf(stderr, "Testing critical section cost (in usec) for 1..%d threads\n", THREADS);
    fprintf(stderr, "Testing:\n");
    for(function_list::iterator it=test_functions.begin(); it!=test_functions.end(); ++it) {
	fprintf(stderr, "\t%s ", it->first);
	runs.resize(runs.size()+1);
	double *times = runs.back().times;
	memset(times, 0, sizeof(run_list));
	for(int j=0; j < SAMPLE_SIZE; j++) {
	    for(int k=1; k <= THREADS; k+=thread_delta) {
		ready = false;
		fprintf(stderr, "\r\t%s %s%c", it->first, dots[j], progress[k%4]);
		local_count = (COUNT+k-1)/k;
		for(long i=0; i < k; i++)
		    pthread_create(&tids[i], NULL, &run, (void*) it->second);
		union {
		    void* vptr;
		    double d;
		} u;
		usleep(100);
		ready = true;
		stopwatch_t timer;
		double total = 0;
		for(long i=0; i < k; i++) {
		    pthread_join(tids[i], &u.vptr);
		    total += u.d;
		}
		double thread_avg = 1e6*(total/k)/local_count/k;
		double wall = 1e6*timer.time()/local_count/k;
		times[k-1] += wall;
		    //		printf("%.3lf %.3lf\n", 1e6*total/COUNT/k, wall);
	    }
	    fprintf(stderr, "\r\t%s %s.", it->first, dots[j]);
	}
	fprintf(stderr, "\n");
    }

    // now print out the results
    fprintf(stderr, "Displaying timing results:\n");
    // first the headers
    printf("Threads");
    for(int i=0; i < test_functions.size(); i++) 
	printf("\t%s", test_functions[i].first);
    printf("\n");
    // now the data
    for(int j=0; j < THREADS; j++) {
	printf("%d", j+1);
	for(int i=0; i < test_functions.size(); i++) 
	    printf("\t%.3lf", runs[i].times[j]/SAMPLE_SIZE);
	printf("\n");
    }
    return 0;
}
