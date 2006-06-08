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
    run_list_t::iterator it = _current_merges.begin();
    while(it != _current_merges.end()) {
        // finished? 
        if(it->_buffer->eof()) {
            // move it to the finished run list
            _finished_merges[it->_merge_level].push_back(it->_file_name);
	    
	    TRACE(TRACE_DEBUG, "Merge output %s done\n", it->_file_name.c_str());

            // delete the merge record and any input files it used
            remove_input_files(it->_buffer);
            it = _current_merges.erase(it);
        }
        else {
            // next!
            ++it;
        }
    }
}



void sort_stage_t::start_merge(sort_stage_t::run_map_t::iterator entry,
                               int merge_factor) {
    int level = entry->first;
    name_list_t &names = entry->second;

    // stores the files associated with this merge run
    name_list_t input_files;
    
    // create an fscan packet for each input run
    buffer_list_t fscan_buffers;
    name_list_t::iterator it = names.begin();
    for(int i=0; i < merge_factor; i++) {
        fscan_packet_t *p;
        tuple_buffer_t *buf = new tuple_buffer_t(_tuple_size);
        p = new fscan_packet_t("sort-fscan", buf,
                               new tuple_filter_t(),
                               _adaptor->get_packet()->client_buffer,
                               it->data());
	TRACE(TRACE_ALWAYS, "Created FSCAN on merge input %s\n", it->data());

        // dispatch the packet
        dispatcher_t::dispatch_packet(p);

        // store the output buffer
        fscan_buffers.push_back(buf);

        // add the file to the merge input list
        input_files.push_back(it->data());

        // remove this element from the list
        it = names.erase(it);
    }
    
    // create a merge packet to consume the fscans
    merge_packet_t *mp;
    tuple_buffer_t *merge_out = new tuple_buffer_t(_tuple_size);
    mp = new merge_packet_t("Sort-merge",
                            merge_out,
                            _adaptor->get_packet()->client_buffer,
                            fscan_buffers,
                            new tuple_filter_t(),
                            merge_factor,
                            _comparator);

    // fire it off
    dispatcher_t::dispatch_packet(mp);

    // last merge run? Send it to the output buffer instead of fdump
    if(_sorting_finished && _current_merges.empty() && _finished_merges.size() == 1) {
        _current_merges.push_back(run_info_t(level+1, "", merge_out));
        _merge_inputs[merge_out] = input_files;
        return;
    }
                
    // create an fdump packet and store its output buffer
    fdump_packet_t *fp;
    tuple_buffer_t *fdump_out;
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
    _current_merges.push_back(run_info_t(level+1, file_name, fdump_out));
    _merge_inputs[fdump_out] = input_files;
}



/**
 * @brief searches each level of the merge hierarchy and fires off a
 * new merge whenever there are MERGE_FACTOR finished runs.
 */
void sort_stage_t::start_new_merges() {
    // start up as many new runs as possible
    run_map_t::iterator it = _finished_merges.begin();
    while(it != _finished_merges.end()) {
        int level = it->first;
        name_list_t &names = it->second;
        while(names.size() >= MERGE_FACTOR)
            start_merge(it, MERGE_FACTOR);

        // special case -- after sorting finishes don't wait for
        // MERGE_FACTOR runs to arrive (they won't). 
        if(names.size() && _sorting_finished) {
            // try to find in-progress runs at or below this level
            unsigned n = 0;
            run_list_t::iterator run=_current_merges.begin();
            while(run != _current_merges.end() && run->_merge_level > level) {
                if(run->_merge_level == level+1)
                    n++;
                
                ++run;
            }
            
            // no in-progress runs?
            if(run == _current_merges.end()) {
                // should we start a partial merge at this level or
                // promote the stragglers? At this point there are n
                // >= 0 (m >= 0) in-progress (finished) merges at the
                // next level. Promote the current batch of stragglers
                // if n+m+1 <= MERGE_FACTOR.
                name_list_t &new_names = _finished_merges[level+1];
                unsigned m = new_names.size();
                if(n + m < MERGE_FACTOR) {
                    // promote the stragglers up a level -- it won't cascade
                    new_names.insert(new_names.end(), names.begin(), names.end());
                    names.clear();
                }
                else {
                    // do a partial merge at this level
                    start_merge(it, names.size());
                }
            }
        }

        // was that the last finished run at this level? erase it
        // so the special case check works right
        run_map_t::iterator old_it = it++;
        if(names.empty()) 
            _finished_merges.erase(old_it);
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
        if(_sorting_finished && _finished_merges.empty() && _current_merges.size() == 1)
            return _current_merges.front()._buffer;
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
        _finished_merges[0].push_back(file_name);
        _sorting_finished = eof;
        _monitor.notify_holding_lock();
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
    file_map_t::iterator it=_merge_inputs.begin();
    while(it != _merge_inputs.end()) {
        tuple_buffer_t *buf = it->first;
        // increment now because remove_input_files() would
        // invalidate the iterator
        ++it;
        remove_input_files(buf);
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



void sort_stage_t::remove_input_files(tuple_buffer_t *buf) {
    // find the input files that fed this buffer
    file_map_t::iterator names = _merge_inputs.find(buf);
    if(names == _merge_inputs.end())
        return;

    // delete the files from disk. The delete will occur as soon as
    // all current file handles are closed
    name_list_t &files = names->second;
    for(name_list_t::iterator it=files.begin(); it != files.end(); ++it) {
        if(remove(it->data()))
            TRACE(TRACE_ALWAYS, "Unable to remove temp file %s", it->data());
	TRACE(TRACE_TEMP_FILE, "Removed finished temp file %s\n", it->data());
    }

    // delete the entry
    _merge_inputs.erase(names);
}
