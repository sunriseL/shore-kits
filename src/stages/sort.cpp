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

void sort_packet_t::terminate_inputs() {
    // close the input buffer
    input_buffer->close();
}

struct tuple_less_t {
    tuple_comparator_t *cmp;
    tuple_less_t(tuple_comparator_t *c) {
        cmp = c;
    }
    bool operator()(const key_tuple_pair_t &a, const key_tuple_pair_t &b) {
        return cmp->compare(a, b);
    }
};

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
        return NULL;
    }

    // open a stream on the file
    FILE *file = fdopen(fd, "w");
    if(!file) {
        TRACE(TRACE_ALWAYS, "Unable to open a stream on %s\n",
              name_template);
        return NULL;
    }

    name = string(name_template);
    return file;
}

/**
 * @brief stores information about an in-progress merge run
 */
struct sort_stage_t::run_info_t {
    int _merge_level;
    string _file_name;
    // this output buffer never sends data -- just watch for eof
    tuple_buffer_t *_buffer;

    run_info_t(int level, const string &name, tuple_buffer_t *buf)
        : _merge_level(level), _file_name(name), _buffer(buf)
    {
    }
};



/**
 * @brief flush (page) to (file) and clear it. PANIC on error.
 */
static void flush_page(tuple_page_t *page, FILE *file,
                       const string &file_name) {
    if(page->fwrite(file) != page->tuple_count()) {
        TRACE(TRACE_ALWAYS, "Error writing sorted run to file\n",
              file_name.data());
        QPIPE_PANIC();
    }
}



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

        // dispatch the packet
        dispatcher_t::dispatch_packet(p, fscan_packet_t::PACKET_TYPE);

        // store the output buffer
        fscan_buffers.push_back(buf);

        // remove this element from the list
        it = names.erase(it);
    }
    
    // create a merge packet to consume the fscans
    merge_packet_t *mp;
    tuple_buffer_t *merge_out;
    merge_out = new tuple_buffer_t(_tuple_size);
    mp = new merge_packet_t("Sort-merge",
                            merge_out,
                            _adaptor->get_packet()->client_buffer,
                            fscan_buffers,
                            new tuple_filter_t(),
                            merge_factor,
                            _comparator);

    // fire it off
    dispatcher_t::dispatch_packet(mp, merge_packet_t::PACKET_TYPE);

    // last merge run? Send it to the output buffer instead of fdump
    if(_sorting_finished && _current_merges.empty() && _finished_merges.size() == 1) {
        _current_merges.push_back(run_info_t(level+1, "", merge_out));
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
                            _adaptor->get_packet()->client_buffer,
                            merge_out,
                            file_name.data());
    dispatcher_t::dispatch_packet(fp, fdump_packet_t::PACKET_TYPE);
    _current_merges.push_back(run_info_t(level+1, file_name, fdump_out));
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

        // special case -- after sorting finishes always merge the
        // lowest level with no in-progress runs to ensure forward
        // progress
        if(_sorting_finished) {
            // try to find in-progress runs for this level (or below)
            run_list_t::iterator run=_current_merges.begin();
            for( ; run != _current_merges.end(); ++run) {
                if(run->_merge_level <= level)
                    break;
            }

            // no in-progress runs?
            if(run == _current_merges.end())
                start_merge(it, _current_merges.size());
                    
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
    critical_section_t cs(&_monitor_lock);
    while(1) {
    
        // wait for a merge to finish
        _merge_finished = false;
        while(!_merge_finished && !_sorting_finished && !_early_termination)
            pthread_cond_wait_wrapper(&_monitor_notify, &_monitor_lock);

        // cancelled?
        if(_early_termination)
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

const size_t sort_stage_t::MERGE_FACTOR = 3;

int sort_stage_t::process_packet(adaptor_t *a) {
    _adaptor = a;
    sort_packet_t *packet = (sort_packet_t *)_adaptor->get_packet();

    tuple_buffer_t *input = packet->input_buffer;
    tuple_comparator_t *compare = packet->compare;

    // create a buffer page for writing to file
    page_guard_t out_page = tuple_page_t::alloc(input->tuple_size, malloc);
    
    // the number of pages per initial run
    int page_count = 3;

    // create a key array 
    int capacity = tuple_page_t::capacity(input->tuple_size,
                                          input->page_size());
    int tuple_count = page_count*capacity;
    array_guard_t<key_tuple_pair_t>
        array = new key_tuple_pair_t[tuple_count];

    // track file names of finished runs, grouped by their level in
    // the hierarchy. When there are enough we'll fire off new merges.
    run_map_t finished_runs;

    // also track a list of unfinished runs to keep an eye on
    run_list_t current_runs;

    // use a monitor thread to control the hierarchical merge
    pthread_t tid;
    thread_t *monitor;
    monitor = member_func_thread(this,
                                 &sort_stage_t::monitor_merge_packets,
                                 "merge monitor");

    if(!thread_create(&tid, monitor)) {
        TRACE(TRACE_ALWAYS, "Unable to create the merge monitor thread");
        QPIPE_PANIC();
    }

    // create sorted runs
    while(!input->wait_for_input()) {
        // TODO: check for early termination at regular intervals
        
        page_list_guard_t page_list;
    
        int index = 0;
        for(int i=0; i < page_count; i++) {
            // read in a run of pages
            tuple_page_t *page = input->get_page();
            if(!page)
                break;

            // add it to the list
            page_list.append(page);

            // add the tuples to the key array
            for(tuple_page_t::iterator it=page->begin(); it != page->end(); ++it) {
                int key = compare->make_key(*it);
                array[index++] = key_tuple_pair_t(key, it->data);
            }
        }

        // sort the key array (gotta love the STL!)
        std::sort(&array[0], &array[index], tuple_less_t(compare));

        // open a temp file to hold the run
        string file_name;
        file_guard_t file = create_tmp_file(file_name);

        // dump the run to file
        for(int i=0; i < index; i++) {
            // write the tuple
            tuple_t out(array[index].data, _tuple_size);
            out_page->append_init(out);

            // flush?
            if(out_page->full())
                flush_page(out_page, file, file_name);
        }
    
        // notify the merge monitor thread that another run is ready
        critical_section_t cs(&_monitor_lock);
        finished_runs[0].push_back(file_name);
        _merge_finished = true;
        pthread_cond_signal_wrapper(&_monitor_notify);
    }
    
    // TODO: skip fdump/merge completely if there is only one run

    // notify the monitor that sorting is complete
    critical_section_t cs(&_monitor_lock);
    _sorting_finished = true;
    pthread_cond_signal_wrapper(&_monitor_notify);
    cs.exit();

    // wait for the output buffer from the final merge to arrive
    tuple_buffer_t *merge_output;
    pthread_join_wrapper(tid, merge_output);
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

