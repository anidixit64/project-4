#include "Join.hpp"

#include <vector>

using namespace std;

/*
 * Input: Disk, Memory, Disk page ids for left relation, Disk page ids for right relation
 * Output: Vector of Buckets of size (MEM_SIZE_IN_PAGE - 1) after partition
 */
vector<Bucket> partition(Disk* disk, Mem* mem, pair<uint, uint> left_rel,
                         pair<uint, uint> right_rel) {
	// output vector
	vector<Bucket> partitions(MEM_SIZE_IN_PAGE - 1, Bucket(disk));

	//when referencing pseudo code in spec, left_rel is R and right_rel is S

	//1. for each disk page in left_rel:
	//	1. load from disk into memory page left_rel_page
	//	2. for each tuple in left_rel_page:
	//			1. hash tuple key
	//			2. load tuple into buffer page specified by hash
	//			3. if buffer page accessed is full, flush buffer to disk and add its disk page id to corresponding bucket.
	//			   then load tuple into now empty page
	//	3. for each memory page:
	//			1. if page isn't empty, flush page to disk and add its disk page id to bucket
	//			(this will flush mem pages that are only partially full in the previous loop)
	//2. reset memory
	//3. do the same for right_rel, with appropriate changes to functions used

	// partitioning left_rel
	for(uint i = left_rel.first; i < left_rel.second; ++i)
	{
		// memory page with id MEM_SIZE_IN_PAGE - 1 is the input buffer
		mem->loadFromDisk(disk, i, MEM_SIZE_IN_PAGE - 1);
		for(uint r = 0; r < mem->mem_page(MEM_SIZE_IN_PAGE - 1)->size(); ++r)
		{
			Record record = mem->mem_page(MEM_SIZE_IN_PAGE - 1)->get_record(r);
			//hash is the mem_page_id of the page where record is going
			//buffer is the pointer to that page
			uint hash = record.partition_hash() % (MEM_SIZE_IN_PAGE - 1);
			Page* buffer = mem->mem_page(hash);
			if(!buffer->full())
			{
				buffer->loadRecord(record);
			}
			else
			{
				partitions[hash].add_left_rel_page(mem->flushToDisk(disk, hash));
				buffer->loadRecord(record);
			}
		}
		
	}

	for(uint m = 0; m < MEM_SIZE_IN_PAGE - 1; ++m)
	{
		Page* p = mem->mem_page(m);
		if(!p->empty())
		{
			partitions[m].add_left_rel_page(mem->flushToDisk(disk, m));
		}
	}

	mem->reset();

	for(uint i = right_rel.first; i < right_rel.second; ++i)
	{
		// memory page with id MEM_SIZE_IN_PAGE - 1 is the input buffer
		mem->loadFromDisk(disk, i, MEM_SIZE_IN_PAGE - 1);
		for(uint r = 0; r < mem->mem_page(MEM_SIZE_IN_PAGE - 1)->size(); ++r)
		{
			Record record = mem->mem_page(MEM_SIZE_IN_PAGE - 1)->get_record(r);
			uint hash = record.partition_hash() % (MEM_SIZE_IN_PAGE - 1);
			Page* buffer = mem->mem_page(hash);
			if(!buffer->full())
			{
				buffer->loadRecord(record);
			}
			else
			{
				partitions[hash].add_right_rel_page(mem->flushToDisk(disk, hash));
				buffer->loadRecord(record);
			}
		}
		
	}

	for(uint m = 0; m < MEM_SIZE_IN_PAGE - 1; ++m)
	{
		Page* p = mem->mem_page(m);
		if(!p->empty())
		{
			partitions[m].add_right_rel_page(mem->flushToDisk(disk, m));
		}
	}

	mem->reset();

	return partitions;
}

/*
 * Input: Disk, Memory, Vector of Buckets after partition
 * Output: Vector of disk page ids for join result
 */
#include <vector>
#include "Disk.hpp"
#include "Mem.hpp"
#include "Bucket.hpp"

using namespace std;

vector<uint> probe(Disk* disk, Mem* mem, vector<Bucket>& partitions) {
    vector<uint> disk_pages;

    for (auto& bucket : partitions) {
        bool use_left = bucket.num_left_rel_record <= bucket.num_right_rel_record;
        
        vector<uint> smaller_rel = use_left ? bucket.get_left_rel() : bucket.get_right_rel();
        vector<uint> bigger_rel = use_left ? bucket.get_right_rel() : bucket.get_left_rel();

        for (uint page_id : smaller_rel) {
            mem->reset();

            mem->loadFromDisk(disk, page_id, MEM_SIZE_IN_PAGE - 1);
            Page* input_buffer = mem->mem_page(MEM_SIZE_IN_PAGE - 1);

            
            for (uint record_idx = 0; record_idx < input_buffer->size(); ++record_idx) {
                Record rec = input_buffer->get_record(record_idx);
                uint hashed_page_id = rec.partition_hash() % (MEM_SIZE_IN_PAGE - 2); 
                
                Page* hash_page = mem->mem_page(hashed_page_id);
                if (hash_page->full()) {
                    mem->flushToDisk(disk, hashed_page_id);
                    hash_page->reset();
                }
                hash_page->loadRecord(rec);
            }

            
            for (uint i = 0; i < MEM_SIZE_IN_PAGE - 2; ++i) {
                Page* hash_page = mem->mem_page(i);
                if (!hash_page->empty()) {
                    mem->flushToDisk(disk, i);
                }
            }
        }

        
        for (uint page_id : bigger_rel) {
            mem->reset(); 

            
            mem->loadFromDisk(disk, page_id, MEM_SIZE_IN_PAGE - 1);
            Page* input_buffer = mem->mem_page(MEM_SIZE_IN_PAGE - 1);

            
            for (uint record_idx = 0; record_idx < input_buffer->size(); ++record_idx) {
                Record big_rec = input_buffer->get_record(record_idx);
                uint hashed_page_id = big_rec.partition_hash() % (MEM_SIZE_IN_PAGE - 2);

                
                Page* hash_page = mem->mem_page(hashed_page_id);
                for (uint hash_rec_idx = 0; hash_rec_idx < hash_page->size(); ++hash_rec_idx) {
                    Record hash_rec = hash_page->get_record(hash_rec_idx);
                    if (big_rec == hash_rec) {
                        Page* output_buffer = mem->mem_page(MEM_SIZE_IN_PAGE - 1); 

                        if (output_buffer->full()) {
                            disk_pages.push_back(mem->flushToDisk(disk, MEM_SIZE_IN_PAGE - 1));
                            output_buffer->reset(); 
                        }
                        output_buffer->loadPair(hash_rec, big_rec); 
                    }
                }
            }
        }

        Page* output_buffer = mem->mem_page(MEM_SIZE_IN_PAGE - 1);
        if (!output_buffer->empty()) {
            disk_pages.push_back(mem->flushToDisk(disk, MEM_SIZE_IN_PAGE - 1));
        }
    }

    return disk_pages;
}

	
	/*
		1. compare number of left and right relations and use the smaller relation for building hash table
		2. get smaller relation's page ids
		3. for each page in smaller relation:
			1. load page from disk to memory
			2. for each tuple in page:
				1. hash tuple key (use modulo MEM_SIZE_IN_PAGE - 2)
				2. load tuple into page with the hashed page_id
			3. flush input buffer to disk
		4. for each page in bigger relation:
			1. load page from disk to memory
			2. for each tuple in page:
				1. hash tuple key (use modulo MEM_SIZE_IN_PAGE - 2)
				2. for each tuple in hashed bucket:
					1. if tuple in bigger relation's page is equal to tuple in hashed bucket:
						1. if output buffer is full
							1. flush output buffer to disk, while pushing the returned disk page id to 
							   disk_pages
						2. load pair into output buffer
				3. for memory page with id < MEM_SIZE_IN_PAGE - 2:
					1. if bucket is not full, flush to disk
			3. flush input buffer to disk
	
	2. return disk_pages
	*/
