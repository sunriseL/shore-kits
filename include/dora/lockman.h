/* -*- mode:C++; c-basic-offset:4 -*- */

/** @file:   lockmap.h
 *
 *  @brief:  Lock manager for DORA partitions
 *
 *  @note:   Enumuration and Compatibility Matrix of locks, 
 *
 *  @author: Ippokratis Pandis, Oct 2008
 *
 */

#ifndef __DORA_LOCK_MAN_H
#define __DORA_LOCK_MAN_H

#include <cstdio>
#include <map>
#include <vector>

#include "dora/dora_error.h"
#include "dora/key.h"

#include "sm/shore/shore_env.h"

#include "stages/tpcc/common/tpcc_random.h"


ENTER_NAMESPACE(dora);


using std::map;
using std::multimap;
using std::vector;


struct ll_entry;
std::ostream& operator<<(std::ostream& os, const ll_entry& rhs);


/******************************************************************** 
 *
 * @enum:  eDoraLockMode
 *
 * @brief: Possible lock types in DORA
 *
 *         - DL_CC_NOLOCK : unlocked
 *         - DL_CC_SHARED : shared lock
 *         - DL_CC_EXCL   : exclusive lock
 *
 ********************************************************************/

enum eDoraLockMode {
    DL_CC_NOLOCK    = 0,
    DL_CC_SHARED    = 1,
    DL_CC_EXCL      = 2,

    DL_CC_MODES     = 3 // @careful: Not an actual lock mode
};

static eDoraLockMode DoraLockModeArray[DL_CC_MODES] =
    { DL_CC_NOLOCK, DL_CC_SHARED, DL_CC_EXCL };



/******************************************************************** 
 *
 * Lock compatibility Matrix
 *
 *
 ********************************************************************/

static int DoraLockModeMatrix[DL_CC_MODES][DL_CC_MODES] = { {1, 1, 1},
                                                            {1, 1, 0},
                                                            {1, 0, 0} };



/******************************************************************** 
 *
 * @struct: ll_entry
 *
 * @brief:  Struct for representing an entry in the lock status map
 *
 * @note:   Each entry has an associated tatas_lock
 *
 ********************************************************************/

struct ll_entry
{
    // data
    eDoraLockMode _ll;     // logical lock
    int          _counter; // transaction counter
    tatas_lock   _lock;    // latch

    ll_entry() : _ll(DL_CC_NOLOCK), _counter(0) { }

    // decrease counter by 1
    // when decreasing counter need to check if it last
    const eDoraLockMode dec_counter() {
        CRITICAL_SECTION(lme_cs, _lock);
        assert (_counter>0);
        --_counter;
        if (_counter==0) _ll = DL_CC_NOLOCK;
        return (_ll);
    }

    // increase counter by 1
    void inc_counter() {
        ++_counter;
        // make sure not to have more than one trxs attached in excl mode
        assert (!((_counter>1)&&(_ll==DL_CC_EXCL))); 
    }

    // friend function
    friend std::ostream& operator<<(std::ostream& os, const ll_entry& rhs);

}; // EOF: struct ll_entry


/******************************************************************** 
 *
 * @struct: key_ll_map_t
 *
 * @brief:  Template-based class for maintaining a map between keys
 *          of the partition and the status of their logical locks.
 *
 *          (Acquire) Returns false if locked in incompatible mode.
 *
 * @note:   It is based on a map of key_wrapper_t to ll_entry
 * @note:   Never removes entries. The map will be increasing as long
 *          as new keys are queried.
 * @note:   We can have a background thread that removes entries from
 *          the map if its size becomes a problem.
 *
 ********************************************************************/

template<class DataType>
class key_ll_map_t
{
public:

    typedef key_wrapper_t<DataType>        Key;
    typedef map<Key, ll_entry>             LLMap;
    typedef typename LLMap::iterator       LLMapIt;
    typedef typename LLMap::const_iterator LLMapCit;

private: 

    // data
    LLMap _ll_map; // map of logical locks - each key has its own ll

public:

    key_ll_map_t() { }

    ~key_ll_map_t() {
        cout << "Keys (" << _ll_map.size() << ")\n";
    }

    // clear map
    void reset() {
        _ll_map.clear();
    }

    // acquire, return true on success
    // false means not compatible
    const bool acquire(const Key& aKey, const eDoraLockMode& adlm) {
        ll_entry* ple = &_ll_map[aKey];
        assert (ple);
        CRITICAL_SECTION(le_cs, ple->_lock);
        // if compatible set new lock mode and increase counter
        if (DoraLockModeMatrix[ple->_ll][adlm]) {
            ple->inc_counter();
            // update the lock mode only if not DL_CC_NO_LOCK
            if (adlm != DL_CC_NOLOCK) ple->_ll = adlm; 
            return (true);
        }
        return (false);
    }
                
    // release        
    const void release(const Key& aKey) {
        ll_entry* ple = &_ll_map[aKey];
        assert (ple);
        ple->dec_counter();
    }

    // for debugging
    void dump() const {
        int sz = _ll_map.size();
        TRACE( TRACE_DEBUG, "Keys (%d)\n", sz);
        for (LLMapCit it=_ll_map.begin(); it!=_ll_map.end(); ++it) {
            cout << "K (" << it->first << ")"; 
            cout << " - L (" << it->second << ")\n";
        }
    }

}; // EOF: struct key_ll_map_t



/******************************************************************** 
 *
 * @class: key_ll_t
 *
 * @brief: Key and lock mode structure
 *
 ********************************************************************/

template<class DataType>
struct key_lm_t
{
    typedef key_wrapper_t<DataType> Key;
    Key*          _pkey;
    eDoraLockMode _lm;

    key_lm_t(Key* akp, eDoraLockMode lm) {
        _pkey = akp;
        _lm = lm;
    }

    ~key_lm_t() { }

}; // EOF: key_lm_t


/******************************************************************** 
 *
 * @class: lock_man_t
 *
 * @brief: Lock manager for the locks of a partition
 *
 * @note:  The lock manager consists of a
 *         - A map for the status of logical locks (llst_map_t)
 *         - A bimap for associating trxs with Keys
 *
 *
 ********************************************************************/

template<class DataType>
class lock_man_t
{
public:

    typedef action_t<DataType>        Action;

    typedef key_wrapper_t<DataType>   Key;
    typedef key_ll_map_t<DataType>    KeyLLMap;
    typedef key_lm_t<DataType>        KeyPrtLmPair;

    typedef vector<Key>               KeyList;
    typedef vector<tid_t>             TidList;

    typedef multimap<tid_t,Key>                 TrxKeyMap;
    typedef typename TrxKeyMap::iterator        TrxKeyMapIt;
    typedef typename TrxKeyMap::const_iterator  TrxKeyMapCit;
    typedef pair<tid_t,Key>                     TrxKeyPair;
    typedef pair<TrxKeyMapIt,TrxKeyMapIt>       TrxRange;

private:

    KeyLLMap  _key_ll_m;   // map of keys to logical locks 
    TrxKeyMap _trx_key_mm; // map of trxs to keys    

    // helper functions
    void _dump_trx_key_mm() const {
        TRACE( TRACE_DEBUG, "(%d) trx-key pairs\n", _trx_key_mm.size());
        for (TrxKeyMapCit cit=_trx_key_mm.begin(); cit!=_trx_key_mm.end(); ++cit) {
            cout << "TRX (" << cit->first << ")";
            cout << " - K (" << cit->second << ")\n";
        }
    }

public:

    lock_man_t() { }

    ~lock_man_t() { 
        cout << "Trx (" << _trx_key_mm.size() << "\n";
    }

    // acquire ll of a key on behalf of a trx
    const bool acquire(tid_t& atid, Key* akey, eDoraLockMode adlm = DL_CC_NOLOCK) {  
        // if lock acquisition successful,
        // associate key to trx
        if (_key_ll_m.acquire(*akey, adlm)) {
            _trx_key_mm.insert(TrxKeyPair(atid, *akey));
            //            dump();
            return (true);
        }
        return (false);
    }

    const bool acquire(tid_t& atid, KeyPrtLmPair& aKeyLmPair) {
        return (acquire(atid, aKeyLmPair._pkey, aKeyLmPair._lm));
    }
                
    // releases all the acquired lls by a trx
    void release(tid_t& atid) {
        TrxRange r = _trx_key_mm.equal_range(atid);
        for (TrxKeyMapIt it=r.first; it!=r.second; ++it) {           
            // release ll for key from the key-to-ll map
            //            cout << "Release - " << (*it).second << endl;
            TRACE( TRACE_TRX_FLOW, "Releasing something from (%d)\n", atid);
            _key_ll_m.release((*it).second);
        }                 
        // remove trx-related entries from the trx-to-key map
        _trx_key_mm.erase(atid);

        //dump();
    }

    // releases action
    void release(Action& a) {
        release(a.get_tid());
    }

    void reset() {
        _key_ll_m.reset();
        _trx_key_mm.clear();
    }

    // for debugging
    void dump() const {
        _key_ll_m.dump();                
        _dump_trx_key_mm();
    }

}; // EOF: struct lock_man_t


EXIT_NAMESPACE(dora);

#endif /* __DORA_LOCK_MAN_H */