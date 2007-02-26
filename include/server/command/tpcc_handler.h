/* -*- mode:C++; c-basic-offset:4 -*- */

#ifndef _TPCC_HANDLER_H
#define _TPCC_HANDLER_H

#include "core.h"
#include "scheduler.h"
#include "workload/driver.h"
#include "workload/driver_directory.h"
#include "workload/workload.h"
#include "server/command/command_handler.h"

#include <map>

using std::map;
using workload::driver_t;



class tpcc_handler_t : public command_handler_t, 
		       public workload::driver_directory_t 
{
    
    // database fields
    enum state_t {
        TPCC_HANDLER_UNINITIALIZED = 0,
        TPCC_HANDLER_INITIALIZED,
        TPCC_HANDLER_SHUTDOWN
    };
    
    static pthread_mutex_t state_mutex;
    static state_t         state;

    
    // command fields
    map<c_str, driver_t*> _drivers;
    map<c_str, scheduler::policy_t*> _scheduler_policies;
    
    void print_usage(const char* command_tag);
    void print_run_statistics(const c_str& desc, 
			      workload::workload_t::results_t &results);

public:

    virtual void init();
    virtual void handle_command(const char* command);
    virtual void shutdown();

    virtual ~tpcc_handler_t() { }

    void add_scheduler_policy(const char*  tag, scheduler::policy_t* dp);
    void add_scheduler_policy(const c_str &tag, scheduler::policy_t* dp);
    
    void add_driver(const char*  tag, driver_t* driver);
    void add_driver(const c_str &tag, driver_t* driver);
    
    driver_t* lookup_driver(const c_str &tag);
};



#endif
