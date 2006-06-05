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

void sort_packet_t::terminate() {
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



static void create_fscan_packets(buffer_list_t &buffers,
                                 name_list_t::iterator begin,
                                 name_list_t::iterator end,
                                 size_t tuple_size)
{
    for(name_list_t::iterator it=begin; it != end; ++it) {
        fscan_packet_t *p;
        tuple_buffer_t *buf = new tuple_buffer_t(tuple_size);
        p = new fscan_packet_t("sort-fscan", buf,
                               new tuple_filter_t,
                               it->data());

        // dispatch the packet
        dispatcher_t::dispatch_packet(p, FSCAN_PACKET_TYPE);

        // store the output buffer
        buffers.push_back(buf);
    }
}

static void start_merge(name_list_t &names, size_t tuple_size,
                        bool eof, size_t merge_factor,
                        tuple_comparator_t *compare,
                        run_map_t &finished_runs,
                        run_list_t &current_runs,
                        int level)
{
    // create an fscan packet for each input run
    buffer_list_t fscan_buffers;
    name_list_t::iterator begin = names.begin();
    name_list_t::iterator end = begin+merge_factor;
    create_fscan_packets(fscan_buffers, begin, end, tuple_size);

    // remove the runs now that we've used them
    names.erase(begin, end);
                
    // create a merge packet to consume the fscans
    merge_packet_t *mp;
    tuple_buffer_t *merge_out;
    merge_out = new tuple_buffer_t(tuple_size);
    mp = new merge_packet_t("Sort-merge",
                            merge_out,
                            fscan_buffers,
                            new tuple_filter_t,
                            merge_factor,
                            compare);

    // fire it off
    dispatcher_t::dispatch_packet(mp, merge_packet_t::TYPE);

    // shortcut the last merge run?
    if(eof && finished_runs.empty() && current_runs.empty()) {
        current_runs.push_back(run_info_t(level+1, "", merge_out));
        return;
    }
                
    // create an fdump packet and store its output buffer
    fdump_packet_t *fp;
    tuple_buffer_t *fdump_out;
    string file_name;
    // KLUDGE! the fdump stage will reopen the file
    fclose(create_tmp_file(file_name));
    fdump_out = new tuple_buffer_t(tuple_size);
    fp = new fdump_packet_t("merge-fdump",
                            fdump_out,
                            merge_out,
                            file_name.data());
    dispatcher_t::dispatch_packet(fp, FDUMP_PACKET_TYPE);
    current_runs.push_back(run_info_t(level+1, file_name, fdump_out));
}

/**
 * @brief checks the status of current and finished merges to see if
 * new merge runs should be started. It eagerly schedules merges as
 * soon as enough runs become available
 *
 * After the sort runs have all been created (eof == true) this
 * function blocks on the earliest, lowest-level merge run until
 * it completes.
 */
static void check_merges(run_map_t &finished_runs,
                         run_list_t &current_runs,
                         bool eof, size_t merge_factor,
                         size_t tuple_size,
                         tuple_comparator_t *compare)
{
    {
        // check for finished runs
        run_list_t::iterator it = current_runs.begin();
        while(it != current_runs.end()) {
            // finished? 
            if(it->buffer->eof()) {
                // move it to the finished run list
                finished_runs[it->merge_level].push_back(it->file_name);
                it = current_runs.erase(it);
            }
            else {
                // next!
                ++it;
            }
        }
    }

    {
        // start up as many new runs as possible
        run_map_t::iterator it = finished_runs.begin();
        while(it != finished_runs.end()) {
            int level = it->first;
            name_list_t &names = it->second;
            while(names.size() >= merge_factor) 
                start_merge(names, tuple_size,
                            eof, merge_factor,
                            compare,
                            finished_runs,
                            current_runs,
                            level);

            // special case -- after eof always merge the lowest level
            // with no in-progress runs to ensure forward progress
            if(eof && it == finished_runs.begin() && names.size()) {
                // try to find in-progress runs for this level
                run_list_t::iterator it=current_runs.begin();
                for( ; it != current_runs.end(); ++it) {
                    if(it->merge_level <= level)
                        break;
                }

                // no in-progress runs?
                if(it == current_runs.end())
                    start_merge(names, tuple_size,
                                eof, current_runs.size(),
                                compare,
                                finished_runs,
                                current_runs,
                                level);
                    
            }

            // was that the last finished run at this level? erase it
            // so the special case check works right
            run_map_t::iterator old_it = it++;
            if(names.empty()) 
                finished_runs.erase(old_it);
        }
    }
}

int sort_stage_t::create_sorted_run(int page_count) {
    // TODO: check for early termination at regular intervals
        
    page_t *page_list = NULL;
    
    int index = 0;
    for(int i=0; i < page_count; i++) {
        // read in a run of pages
        tuple_page_t *page = input->get_page();
        if(!page)
            break;

        // add it to the list
        page->next = page_list;
        page_list = page;

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
    FILE *file = create_tmp_file(file_name);

    // allocate a page to use as a file buffer
    tuple_page_t *out_page = tuple_page_t::alloc(_tuple_size, malloc);
    out_page->next = page_list;
    page_list = out_page;
    
    // dump the run to file
    for(int i=0; i < index; i++) {
        // write the tuple
        tuple_t out(array[index].data, _tuple_size);
        out_page->append_init(out);

        // flush?
        if(out_page->full())
            flush_page(out_page, file, file_name);
    }
    
    // add to the list of finished runs
    finished_runs[0].push_back(file_name);

    // close the file and free the buffer
    fclose(file);

    // free the page list
    while(page_list) {
        page_t *page = page_list;
        page_list = page->next;
        free(page);
    }

    // run finished
    return 0;
}

int sort_stage_t::process_packet(packet_t *p) {
    sort_packet_t *packet = (sort_packet_t *)p;

    // automatically close the input buffer when this function exits
    buffer_guard_t input(packet->input_buffer);
    tuple_comparator_t *compare = packet->compare;

    // create a page for writing to file
    tuple_page_t *out_page = tuple_page_t::alloc(input->tuple_size, malloc);
    
    // the number of pages per initial run
    int page_count = 3;
    int merge_factor = 3;

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

    // go!
    while(1) {
        if(create_sorted_run(page_count))
            break;
        
        // make sure we detect eof before firing off the last merge
        if(input->wait_for_input())
            break;

        // start a merge?
        check_merges(finished_runs, current_runs, false,
                     merge_factor, input->tuple_size, compare);
    }
    
    // TODO: short-cut the fdump if there is only one run

    // wait for the top-level merge to start
    while(current_runs.size() > 1 || finished_runs.size())
        check_merges(finished_runs, current_runs, true,
                     merge_factor, input->tuple_size, compare);

    // the last merge does not go to file. transfer its output
    // buffer's pages directly to my output buffer.
    run_info_t final_merge = current_runs.front();
    current_runs.pop_front();
    assert(current_runs.empty());
    
    buffer_guard_t proxy(final_merge.buffer);
    tuple_t out;
    while(proxy->get_tuple(out))
        if(output(packet, out))
            return 1;

    return 0;
}

