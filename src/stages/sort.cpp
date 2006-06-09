// -*- mode:C++; c-basic-offset:4 -*-

#include "stages/sort.h"
#include "stages/merge.h"
#include "stages/fdump.h"
#include "stages/fscan.h"
#include "trace/trace.h"
#include "qpipe_panic.h"
#include "utils.h"
#include <algorithm>
#include <string>
#include <cstdlib>

#include <deque>
#include <map>
#include <list>

using std::string;
using std::deque;
using std::map;
using std::list;



struct tuple_less_t {
    tuple_comparator_t *cmp;
    tuple_less_t(tuple_comparator_t *c) {
        cmp = c;
    }
    bool operator()(const key_tuple_pair_t &a, const key_tuple_pair_t &b) {
        return cmp->compare(a, b) < 0;
    }
};



const char* sort_packet_t::PACKET_TYPE = "SORT";



const char* sort_stage_t::DEFAULT_STAGE_NAME = "SORT_STAGE";



const size_t sort_stage_t::MERGE_FACTOR = 3;



static FILE *create_tmp_file(string &name);
static void flush_page(tuple_page_t *page, FILE *file,
                       const string &file_name);



/**
 * @brief checks for finished merges and moves them to the finished
 * merge map so their results can be merged in turn.
 */
void sort_stage_t::check_finished_merges() {
    // check for finished merges and 
    merge_map_t::iterator level_it = _merge_map.begin();
    while(level_it != _merge_map.end()) {
        int level = level_it->first;
        merge_list_t &merges = level_it->second;
        merge_list_t::iterator it = merges.begin();
        while(it != merges.end()) {
            // finished? 
            if(it->_signal_buffer->eof()) {

                // move it to the finished run list
                _run_map[level].push_back(it->_output);
                TRACE(TRACE_DEBUG, "Added finished merge file %s to _run_map[%d]\n",
                      it->_output.c_str(),
                      level);

                // delete the merge record and any input files it used
                remove_input_files(it->_inputs);
                it = merges.erase(it);
            }
            else {
                // next!
                ++it;
            }
        }

        // if that was the last merge at this level, remove it
        merge_map_t::iterator old_level_it = level_it++;
        if(merges.empty())
            _merge_map.erase(old_level_it);
    }
}



void sort_stage_t::start_merge(int new_level, run_list_t &runs, int merge_factor)
{

    assert(merge_factor > 0);
    assert(runs.size() >= (unsigned int)merge_factor);


    // allocate a new merge
    merge_list_t &merges = _merge_map[new_level];
    merges.resize(merges.size()+1);
    merge_t &merge = merges.back();


    run_list_t merge_inputs;
    buffer_list_t fscan_buffers;


    run_list_t::iterator it = runs.begin();
    for(int i = 0; i < merge_factor; i++) {

        // grab one of our runs
	string run_filename = *it;
        it = runs.erase(it);
	
        fscan_packet_t* p;
        tuple_buffer_t* buf = new tuple_buffer_t(_tuple_size);
        p = new fscan_packet_t("sort-fscan",
			       buf,
                               new tuple_filter_t(),
                               _adaptor->get_packet()->client_buffer,
                               run_filename.c_str());
        dispatcher_t::dispatch_packet(p);

	merge._inputs.push_back(run_filename.c_str());
	fscan_buffers.push_back(buf);
    }


    // create a merge packet to consume the fscans
    merge_packet_t* mp;
    tuple_buffer_t* merge_out = new tuple_buffer_t(_tuple_size);
    mp = new merge_packet_t("Sort-merge",
                            merge_out,
                            _adaptor->get_packet()->client_buffer,
                            fscan_buffers,
                            new tuple_filter_t(),
                            merge_factor,
                            _comparator);
    dispatcher_t::dispatch_packet(mp);


    // if this is the last merge run, send output to SORT worker
    // thread, not to an FDUMP
    if(new_level < 0) {
        merge._signal_buffer = merge_out;
        return;
    }
    
    
    // otherwise, redirect merge to an FDUMP so we create a new run on
    // disk
    fdump_packet_t* fp;
    tuple_buffer_t* fdump_out;
    string file_name;
    // KLUDGE! the fdump stage will reopen the file
    fclose(create_tmp_file(file_name));
    fdump_out = new tuple_buffer_t(_tuple_size);
    fp = new fdump_packet_t("merge-fdump",
                            fdump_out,
                            merge_out,
                            _adaptor->get_packet()->client_buffer,
                            file_name.data(),
                            &_monitor);
    dispatcher_t::dispatch_packet(fp);


    merge._output = file_name;
    merge._signal_buffer = fdump_out;
}



/**
 * @brief searches each level of the merge hierarchy and fires off a
 * new merge whenever there are MERGE_FACTOR finished runs.
 */
void sort_stage_t::start_new_merges() {

    
    TRACE(TRACE_DEBUG, "_run_map has size %d\n",
	  _run_map.size());
    

    // start up as many new runs as possible
    run_map_t::iterator level_it = _run_map.begin();
    while(level_it != _run_map.end()) {

        int level = level_it->first;
	TRACE(TRACE_ALWAYS, "Running on level %d\n", level);

        
        run_list_t &runs = level_it->second;
        bool started_merges = false;
	while(runs.size() >= MERGE_FACTOR) {
	    // "normal" k-way merges
            start_merge(level+1, runs, MERGE_FACTOR);
            started_merges = true;
        }

        // increment the iterator. rename for clarity
        run_map_t::iterator curr_level_it = level_it++;
        run_map_t::iterator next_level_it = level_it;

	TRACE(TRACE_DEBUG, "curr_level = %d ; next_level = %d\n",
	      curr_level_it->first,
	      next_level_it->first);
        

        // If we have no runs on this level, erase this level's
        // _run_map entry. Now a level has no runs if no mapping
        // exists, not if a mapping exists to a set of size 0.
        if(runs.empty()) {
            _run_map.erase(curr_level_it);
            continue;
        }
        
        // special case -- if sorting has finished, we can't
        // necessarily wait for MERGE_FACTOR runs to arrive (they may
        // never come)
        if(!_sorting_finished || started_merges)
            continue;


	// Try to find (materialized) runs above this
        // level. 'next_level_it' is set to -1 if no runs exist.
        // Otherwise, it is set to the lowest level (above us) with
        // such runs available.
        int next_run_level =
	    (next_level_it == _run_map.end()) ? -1 : next_level_it->first;

	
        // Try to find in-progress merges at or below this
        // level. 'lowest_merge_level' is set to -1 if no merges are
        // found. Otherwise, it is set to the lowest level where a
        // merge is taking place.
        merge_map_t::iterator merges = _merge_map.begin();
        int lowest_merge_level = (merges == _merge_map.end())? -1 : merges->first;


	TRACE(TRACE_DEBUG, "lowest_merge_level = %d ; next_run_level = %d\n",
	      lowest_merge_level,
	      next_run_level);

	
        // We reduce system I/O when we merge small files together
        // before moving them up the merge hierarchy.


        int next_size, next_level;
	if ((lowest_merge_level >= 0) && (lowest_merge_level <= level)) {
	    // There are merges happening below us. Wait for them to
	    // finish. We really want to merge with the result of
	    // every merge below before shipping work to higher levels
	    // of merge hierarchy.
	    continue;
	}
	else if ((lowest_merge_level < 0) && (next_run_level < 0)) {
	    // No merges taking place in the system and no runs above
	    // us. Submit one final merge request. start_merge()
	    // should redirect output to the SORT worker thread and
	    // optimize for the single-merge case.
	    start_merge(-1, runs, runs.size());
	    _run_map.erase(curr_level_it);
	    continue;
	}
	else if (next_run_level < 0) {
	    // No runs above us, but there are merges taking place!
	    // Move up to the output level of the lowest merge.
	    next_level = lowest_merge_level;
	    next_size  = merges->second.size();
	}
        else if(lowest_merge_level > next_run_level) {
            // only runs at the next level
            next_level = next_run_level;
            next_size  = next_level_it->second.size();
        }
        else if(lowest_merge_level < next_run_level) {
            // only merges at the next level
            next_level = lowest_merge_level;
            next_size  = merges->second.size();
        }
        else {
            // both runs and merges at the next level
            next_level = lowest_merge_level;
            next_size = merges->second.size() + next_level_it->second.size(); 
	}


	TRACE(TRACE_DEBUG, "next_size = %d ; next_level = %d\n",
	      next_size,
	      next_level);

                
        // promote or partial merge?
        int required_merges = (next_size + MERGE_FACTOR-1)/MERGE_FACTOR;
        int potential_merges = (next_size + runs.size() + MERGE_FACTOR-1)/MERGE_FACTOR;
        if(potential_merges > required_merges) {
            // partial merge
            start_merge(next_level, runs, runs.size());
        }
        else {
            // promote up
            run_list_t &next_runs = _run_map[next_level];
            next_runs.insert(next_runs.end(), runs.begin(), runs.end());
        }

        // no more runs at this level. Erase it so the special case
        // check works right
        _run_map.erase(curr_level_it);
    }
}



/**
 * @brief Executed by a separate thread to manage the hierachical
 * merge. Spends most of its time blocked, so very little overhead --
 * we just need a way to detect asynchronous merge completions and
 * handle them.
 */
tuple_buffer_t *sort_stage_t::monitor_merge_packets() {
    // always in a critical section, but usually blocked on cond_wait
    critical_section_t cs(&_monitor._lock);
    while(1) {
    
        // wait for a merge to finish
        if(_monitor.wait_holding_lock())
            return NULL;

        // find the finished merge run(s)
        check_finished_merges();

        // fire off new merges as needed
        start_new_merges();

        // have we started the final merge? The worker thread will take care of it
        merge_map_t::iterator merge = _merge_map.begin();
        if(merge != _merge_map.end() && merge->first < 0)
            return merge->second.front()._signal_buffer;
    }
}



int sort_stage_t::process_packet() {

    sort_packet_t *packet = (sort_packet_t *)_adaptor->get_packet();

    _input = packet->input_buffer;
    _comparator = packet->compare;
    _tuple_size = _input->tuple_size;
    _sorting_finished = false;

    // do we even have any inputs?
    _input->init_buffer();
    if(_input->wait_for_input())
        return 0;
    
    // create a buffer page for writing to file
    page_guard_t out_page = tuple_page_t::alloc(_tuple_size, malloc);
    
    // the number of pages per initial run
    int page_count = 3;

    // create a key array 
    int capacity = tuple_page_t::capacity(_input->page_size(),
                                          _tuple_size);
    int tuple_count = page_count*capacity;
    key_vector_t array;
    array.reserve(tuple_count);


    // use a monitor thread to control the hierarchical merge
    thread_t *monitor;
    monitor = member_func_thread(this,
                                 &sort_stage_t::monitor_merge_packets,
                                 "merge monitor");
    if(thread_create(&_monitor_thread, monitor)) {
        TRACE(TRACE_ALWAYS, "Unable to create the merge monitor thread");
        QPIPE_PANIC();
    }


    // create sorted runs
    do {
        // TODO: check for early termination at regular intervals
        
        page_list_guard_t page_list;
        array.clear();
        for(int i=0; i < page_count; i++) {
            // read in a run of pages
            tuple_page_t *page = _input->get_page();
            if(!page)
                break;

            // add it to the list
            page_list.append(page);

            // add the tuples to the key array
            for(tuple_page_t::iterator it=page->begin(); it != page->end(); ++it) {
                int key = _comparator->make_key(*it);
                array.push_back(key_tuple_pair_t(key, it->data));
            }
        }

        // sort the key array (gotta love the STL!)
        std::sort(array.begin(), array.end(), tuple_less_t(_comparator));

        // open a temp file to hold the run
        string file_name;
        file_guard_t file = create_tmp_file(file_name);

        // dump the run to file
        //        for(int i=0; i < index; i++) {
        for(key_vector_t::iterator it=array.begin(); it != array.end(); ++it) {
            // write the tuple
            tuple_t out(it->data, _tuple_size);
            out_page->append_init(out);

            // flush?
            if(out_page->full())
                flush_page(out_page, file, file_name);
        }

        // make sure to pick up the stragglers
        if(!out_page->empty())
            flush_page(out_page, file, file_name);

        // are we done?
        bool eof = _input->wait_for_input();
        
        // notify the merge monitor thread that another run is ready
        critical_section_t cs(&_monitor._lock);
	run_list_t &runs = _run_map[0];
        runs.push_back(file_name);
        _sorting_finished = eof;
        _monitor.notify_holding_lock();

	TRACE(TRACE_DEBUG, "Added file %s to _run_map[0]\n", file_name.c_str());

    } while(!_sorting_finished);
    
    // TODO: skip fdump/merge completely if there is only one run

    // wait for the output buffer from the final merge to arrive
    tuple_buffer_t *merge_output;
    pthread_join_wrapper(_monitor_thread, merge_output);
    _monitor_thread = 0;
    if(merge_output == NULL) {
        TRACE(TRACE_ALWAYS, "Merge failed. Bailing out early.");
        return 1;
    }
    
    // transfer the output of the last merge to the stage output
    tuple_t out;
    while(merge_output->get_tuple(out))
        if(_adaptor->output(out))
            return 1;

    return 0;
}



sort_stage_t::~sort_stage_t()  {
    // make sure the monitor thread exits before we do...
    if(_monitor_thread && pthread_join(_monitor_thread, NULL)) {
        TRACE(TRACE_ALWAYS, "sort stage unable to join on monitor thread");
        QPIPE_PANIC();
    }

    // also, remove any remaining temp files
    merge_map_t::iterator level_it=_merge_map.begin();
    while(level_it != _merge_map.end()) {
        merge_list_t::iterator it = level_it->second.begin();
        while(it != level_it->second.end()) {
            remove_input_files(it->_inputs);
            ++it;
        }
        ++level_it;
    }
}



/**
 * @brief flush (page) to (file) and clear it. PANIC on error.
 */
static void flush_page(tuple_page_t *page, FILE *file,
                       const string &file_name) {
    if(page->fwrite_full_page(file)) {
        TRACE(TRACE_ALWAYS, "Error writing sorted run to file\n",
              file_name.data());
        QPIPE_PANIC();
    }
    page->clear();
}



/**
 * @brief opens a temporary file with a unique name
 *
 * @param name string to store the new file's name in
 *
 * @return the file or NULL on error
 */
static FILE *create_tmp_file(string &name) {
    // TODO: use a configurable temp dir
    char name_template[] = "tmp/sort-run.XXXXXX";
    int fd = mkstemp(name_template);
    if(fd < 0) {
        TRACE(TRACE_ALWAYS, "Unable to open temporary file %s!\n",
              name_template);
        QPIPE_PANIC();
    }
    TRACE(TRACE_TEMP_FILE, "Created temp file %s\n", name_template);


    // open a stream on the file
    FILE *file = fdopen(fd, "w");
    if(!file) {
        TRACE(TRACE_ALWAYS, "Unable to open a stream on %s\n",
              name_template);
        QPIPE_PANIC();
    }

    name = string(name_template);
    return file;
}

int sort_stage_t::print_runs() {
    run_map_t::iterator level_it = _run_map.begin();
    for( ; level_it != _run_map.end(); ++level_it) {
        int level = level_it->first;
        run_list_t &runs = level_it->second;
        printf("Level %d (%d):\n", level, runs.size());
        run_list_t::iterator it = runs.begin();
        for( ; it != runs.end(); ++it)
            printf("\t%s\n", it->c_str());
    }
    return 0;
}

int sort_stage_t::print_merges() {
    merge_map_t::iterator level_it = _merge_map.begin();
    for( ; level_it != _merge_map.end(); ++level_it) {
        int level = level_it->first;
        merge_list_t &merges = level_it->second;
        printf("Level %d (%d)\n", level, merges.size());
        merge_list_t::iterator it = merges.begin();
        for( ; it != merges.end(); ++it) {
            printf("\t%s <=\n", it->_output.c_str());
            run_list_t::iterator run_it = it->_inputs.begin();
            for( ; run_it != it->_inputs.end(); ++run_it) 
                printf("\t\t%s\n", run_it->c_str());
        }
    }
    return 0;
}



void sort_stage_t::remove_input_files(run_list_t &files) {
    // delete the files from disk. The delete will occur as soon as
    // all current file handles are closed
    for(run_list_t::iterator it=files.begin(); it != files.end(); ++it) {
        if(remove(it->c_str()))
            TRACE(TRACE_ALWAYS, "Unable to remove temp file %s", it->c_str());
	TRACE(TRACE_TEMP_FILE, "Removed finished temp file %s\n", it->c_str());
    }
}
