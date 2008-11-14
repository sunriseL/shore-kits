/* -*- mode:C++; c-basic-offset:4 -*- */

/** @file:   tester_shore_shell.h
 *
 *  @brief:  Abstract shell class for testing Shore environments 
 *
 *  @author: Ippokratis Pandis, Sept 2008
 */

#ifndef __TESTER_SHORE_SHELL_H
#define __TESTER_SHORE_SHELL_H

#ifdef __SUNPRO_CC
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#else
#include <cstdlib>
#include <cstdio>
#include <cstring>
#endif

#include "util/shell.h"
#include "tests/common/tester_shore_client.h"
#include "sm/shore/shore_env.h"

using std::map;


extern "C" void alarm_handler(int sig);

extern bool volatile _g_canceled;



/*********************************************************************
 *
 *  @class: shore_guard_t
 *
 *  @brief: Ensures that the Shore environment gets closed
 *
 *********************************************************************/

struct shore_guard_t : pointer_guard_base_t<ShoreEnv, shore_guard_t> 
{
    shore_guard_t(ShoreEnv *ptr=NULL)
        : pointer_guard_base_t<ShoreEnv, shore_guard_t>(ptr)
    { }

    static void guard_action(ShoreEnv *ptr) 
    {
        if (ptr) {
            close_smt_t* clt = new close_smt_t(ptr, c_str("clt"));
            assert (clt);
            clt->fork();
            clt->join();
            int rv = clt->_rv;
            delete (clt);
            clt = NULL;
            if (clt->_rv) {
                TRACE( TRACE_ALWAYS, "Error in closing thread...\n");
            }
        }
    }
};


/*********************************************************************
 *
 *  @abstract class: shore_shell_t
 *
 *  @brief: Base class for shells for Shore environment
 *
 *  @usage: - Inherit from this class
 *          - Implement the process_{TEST/MEASURE/WARMUP} fuctions
 *          - Call the start() function
 *
 *
 *  @note:  Supported commands - { TEST/MEASURE/WARMUP/LOAD }
 *  @note:  To add new command function process_command() should be overridden 
 *  @note:  SIGINT handling
 *
 *********************************************************************/

class shore_shell_t : public shell_t 
{
protected:

    guard<ShoreEnv> _env;
    processorid_t _start_prs_id;
    processorid_t _current_prs_id;    

    // supported trxs
    typedef map<int,string>            mapSupTrxs;
    typedef mapSupTrxs::iterator       mapSupTrxsIt;
    typedef mapSupTrxs::const_iterator mapSupTrxsConstIt;

    mapSupTrxs _sup_trxs;

    // supported binding policies
    typedef map<eBindingType,string>            mapBindPols;
    typedef mapBindPols::iterator               mapBindPolsIt;    
    mapBindPols _sup_bps;

public:

    shore_shell_t(const char* prompt, processorid_t acpustart = PBIND_NONE) 
        : shell_t(prompt), 
          _env(NULL), _start_prs_id(acpustart), _current_prs_id(acpustart)
    {
        // install signal handler
        struct sigaction sa;
        struct sigaction sa_old;
        sa.sa_flags = 0;
        sigemptyset(&sa.sa_mask);
        sa.sa_handler = &alarm_handler;
        if(sigaction(SIGALRM, &sa, &sa_old) < 0)
            exit(1);
    }
    virtual ~shore_shell_t() { }    

    // access methods
    ShoreEnv* db() { return(_env.get()); }

    // shell interface
    virtual int process_command(const char* command, const char* command_tag);
    virtual int print_usage(const char* command);
    virtual int SIGINT_handler();

    // supported commands and their usage
    virtual int process_cmd_TEST(const char* command, const char* command_tag);
    virtual int process_cmd_MEASURE(const char* command, const char* command_tag);
    virtual int process_cmd_WARMUP(const char* command, const char* command_tag);    
    virtual int process_cmd_LOAD(const char* command, const char* command_tag);        
    virtual int process_cmd_TRXS(const char* command, const char* command_tag); 
    //    virtual int process_cmd_RESTART(const char* command, const char* command_tag)=0;

    virtual void usage_cmd_TEST();
    virtual void usage_cmd_MEASURE();
    virtual void usage_cmd_WARMUP();    
    virtual void usage_cmd_LOAD();    

    virtual const int register_commands()=0;


    // Instanciate and close the Shore environment
    virtual const int inst_test_env(int argc, char* argv[])=0;

    // supported trxs and binding policies
    virtual const int load_trxs_map(void)=0;
    virtual const int load_bp_map(void)=0;
    void print_sup_trxs(void) const;
    void print_sup_bp(void);
    const char* translate_trx(const int iSelectedTrx) const;
    const char* translate_bp(const eBindingType abt);


    // virtual implementation of the {WARMUP/TEST/MEASURE} 
    // WARMUP/LOAD are virtual
    // TEST/MEASURE are pure virtual
    virtual int _cmd_WARMUP_impl(const int iQueriedWHs, const int iTrxs, 
                                 const int iDuration, const int iIterations);
    virtual int _cmd_LOAD_impl(void);

    virtual int _cmd_TEST_impl(const int iQueriedWHs, const int iSpread,
                               const int iNumOfThreads, const int iNumOfTrxs,
                               const int iSelectedTrx, const int iIterations,
                               const int iUseSLI, const eBindingType abt)=0;
    virtual int _cmd_MEASURE_impl(const int iQueriedWHs, const int iSpread,
                                  const int iNumOfThreads, const int iDuration,
                                  const int iSelectedTrx, const int iIterations,
                                  const int iUseSLI, const eBindingType abt)=0;    

    // for the client processor binding policy
    virtual processorid_t next_cpu(const eBindingType abt,
                                   const processorid_t aprd);


protected:

    virtual void print_throughput(const int iQueriedWHs, const int iSpread, 
                                  const int iNumOfThreads, const int iUseSLI, 
                                  const double delay, const eBindingType abt) { /* default do nothing */ };
    
    void print_MEASURE_info(const int iQueriedWHs, const int iSpread, 
                            const int iNumOfThreads, const int iDuration,
                            const int iSelectedTrx, const int iIterations,
                            const int iUseSLI, const eBindingType abt);

    void print_TEST_info(const int iQueriedWHs, const int iSpread, 
                         const int iNumOfThreads, const int iNumOfTrxs,
                         const int iSelectedTrx, const int iIterations,
                         const int iUseSLI, const eBindingType abt);

}; // EOF: shore_shell_t



#endif /* __TESTER_SHORE_SHELL_H */
