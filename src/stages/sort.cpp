// -*- mode:C++; c-basic-offset:4 -*-

#include "stages/sort.h"
#include "trace/trace.h"
#include <algorithm>
#include <string>
#include <cstdlib>
#include <vector>
#include <map>
#include <list>

using std::string;
using std::vector;
using std::map;
using std::list;

void sort_packet_t::terminate() {
    // close the input buffer
    input_buffer->close();
}

struct page_list_guard_t {
    page_t *_head;
    page_list_guard_t()
        : _head(NULL)
    {
    }
    ~page_list_guard_t() {
        while(_head) {
            page_t *page = _head;
            _head = page->next;
            free(page);
        }
    }
    void append(page_t *page) {
        page->next = _head;
        _head = page;
    }
};


struct sort_comparator {
    tuple_comparator_t *cmp;
    
    bool operator()(const key_tuple_pair_t &a, const key_tuple_pair_t &b) {
        return cmp->compare(a, b);
    }
    
    sort_comparator(tuple_comparator_t *c)
        : cmp(c)
    {
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
 * @brief flush (page) to (file) and clear it. PANIC on error.
 */
static void flush_page(tuple_page_t *page, FILE *file) {
    if(page->fwrite(file) != page->tuple_count()) {
        TRACE(TRACE_ALWAYS, "Error writing sorted run to file\n",
              name_template);
        QPIPE_PANIC();
    }
}



/**
 * @brief stores information about an in-progress merge run
 */
struct run_info_t {
    int merge_level;
    string file_name;
    // this output buffer never sends data -- just watch for eof
    tuple_buffer_t *buffer;

    run_info_t(int level, const string &name, tuple_buffer_t *buf)
        : merge_level(level), file_name(name), buffer(buf)
    {
    }
};



/**
 * @brief checks the status of current and finished merges to see if
 * new merge runs should be started.
 */
static void check_merges(map<int, vector<string> > &finished_runs,
                         list<run_info_t> &current_runs,
                         bool eof)
{
    // TODO
}

int sort_stage_t::process_packet(packet_t *p) {
    sort_packet_t *packet = (sort_packet_t *)p;

    // automatically close the input buffer when this function exits
    buffer_closer_t input = packet->input_buffer;
    tuple_comparator_t *compare = packet->compare;

    // create a page for writing to file
    tuple_page_t *out_page = tuple_page_t::alloc(input->tuple_size);
    
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
    map< int, vector<string> > finished_runs;

    // also track a list of unfinished runs to keep an eye on
    list<run_info_t> current_runs;

    // go!
    while(1) {
        
        // delete the pages in this run after we finish with them
        page_list_guard_t page_list;
        int index = 0;
        for(int i=0; i < page_count; i++) {
            // read in a run of pages
            tuple_page_t *page = input->get_page();
            if(!page)
                break;

            // add it to the list
            page_list.append(page);
            for(tuple_page_t::iterator it=page->begin(); it != page->end(); ++it) {
                int key = compare->make_key(*it);
                array[index++] = key_tuple_pair_t(key, it->data);
            }
        }

        // sort them (gotta love the STL!)
        std::sort(&array[0], &array[index], sort_comparator(compare));

        // save the temp file's name for later
        string file_name;
        FILE *file = create_tmp_file(file_name);
        runs.push_back(file_name);

        // dump the run to file
        tuple_t out_tup;
        for(int i=0; i < index; i++) {
            // write the tuple
            out_page.alloc_tuple(out_tup);
            out_tup.data = array[index].data;

            // flush?
            if(out_page->full())
                flush_page(out_page, file);
        }

        // close the file
        fclose(file);

        // add to the finished run list
        vector<string> &level0 = finished_runs[0];
        level0.push_back(file_name);

        // make sure we detect eof before firing off the last merge
        if(input->wait_for_input())
            break;

        // start a merge?
        check_merges(finished_runs, current_runs, false);
    }
    
    // TODO: short-cut the fdump if there is only one run

    // wait for the top-level merge to start
    while(current_runs.size() > 1 || finished_runs.size())
        check_merges(finished_runs, current_runs, true);

    // the last merge does not go to file. transfer its output
    // buffer's pages directly to my output buffer.
    run_info_t final_merge = current_runs.front();
    current_runs.pop_front();
    assert(current_runs.empty());
    
    buffer_guard_t proxy = final_merge.buffer;
    tuple_t out;
    while(proxy->get_tuple(out))
        if(output(packet, tuple))
            return 1;

    return 0;
}

