/* -*- mode:C++; c-basic-offset:4 -*- */

#include "workload/tpch/tpch_env.h"

DbEnv* dbenv = NULL;

// tables
page_list* tpch_customer = new page_list();
page_list* tpch_lineitem = new page_list();
page_list* tpch_nation = new page_list();
page_list* tpch_orders = new page_list();
page_list* tpch_part = new page_list();
page_list* tpch_partsupp = new page_list();
page_list* tpch_region = new page_list();
page_list* tpch_supplier = new page_list();

// indexes (*_idx are unassociated and only return primary keys)
Db* tpch_lineitem_shipdate = NULL;
Db* tpch_lineitem_shipdate_idx = NULL;

