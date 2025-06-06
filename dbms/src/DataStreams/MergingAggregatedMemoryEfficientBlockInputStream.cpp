// Copyright 2023 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <Common/CurrentMetrics.h>
#include <Common/setThreadName.h>
#include <Common/wrapInvocable.h>
#include <DataStreams/MergingAggregatedMemoryEfficientBlockInputStream.h>

#include <future>

namespace DB
{
/** Scheme of operation:
  *
  * We have to output blocks in specific order: by bucket number:
  *
  *  o o o o ... o
  *  0 1 2 3     255
  *
  * Each block is the result of merge of blocks with same bucket number from several sources:
  *
  *  src1   o o ...
  *         | |
  *  src2   o o
  *
  *         | |
  *         v v
  *
  *  result o o
  *         0 1
  *
  * (we must merge 0th block from src1 with 0th block from src2 to form 0th result block and so on)
  *
  * We may read (request over network) blocks from different sources in parallel.
  * It is done by getNextBlocksToMerge method. Number of threads is 'reading_threads'.
  *
  * Also, we may do merges for different buckets in parallel.
  * For example, we may
  *      merge 1th block from src1 with 1th block from src2 in one thread
  *  and merge 2nd block from src1 with 2nd block from src2 in other thread.
  * Number of threads is 'merging_threads'
  * And we must keep only 'merging_threads' buckets of blocks in memory simultaneously,
  *  because our goal is to limit memory usage: not to keep all result in memory, but return it in streaming form.
  *
  * So, we return result sequentially, but perform calculations of resulting blocks in parallel.
  *  (calculation - is doing merge of source blocks for same buckets)
  *
  * Example:
  *
  *  src1   . . o o . . .
  *             | |
  *  src2       o o
  *
  *             | |
  *             v v
  *
  *  result . . o o . . .
  *
  * In this picture, we do only two merges in parallel.
  * When a merge is done, method 'getNextBlocksToMerge' is called to get blocks from sources for next bucket.
  * Then next merge is performed.
  *
  * Main ('readImpl') method is waiting for merged blocks for next bucket and returns it.
  */


MergingAggregatedMemoryEfficientBlockInputStream::MergingAggregatedMemoryEfficientBlockInputStream(
    BlockInputStreams inputs_,
    const Aggregator::Params & params,
    bool final_,
    size_t reading_threads_,
    size_t merging_threads_,
    const String & req_id)
    : log(Logger::get(req_id))
    , aggregator(
          params,
          req_id,
          merging_threads_,
          [](const OperatorSpillContextPtr &) {},
          /*is_auto_pass_through=*/false,
          params.use_magic_hash)
    , final(final_)
    , reading_threads(std::min(reading_threads_, inputs_.size()))
    , merging_threads(merging_threads_)
    , inputs(inputs_.begin(), inputs_.end())
{
    children = inputs_;

    /** Create threads that will request and read data from remote servers.
      */
    if (reading_threads > 1)
        reading_pool = std::make_unique<legacy::ThreadPool>(reading_threads);

    /** Create threads. Each of them will pull next set of blocks to merge in a loop,
      *  then merge them and place result in a queue (in fact, ordered map), from where we will read ready result blocks.
      */
    if (merging_threads > 1)
        parallel_merge_data = std::make_unique<ParallelMergeData>(merging_threads);
}


Block MergingAggregatedMemoryEfficientBlockInputStream::getHeader() const
{
    return aggregator.getHeader(final);
}


void MergingAggregatedMemoryEfficientBlockInputStream::readPrefix()
{
    start();
}


void MergingAggregatedMemoryEfficientBlockInputStream::readSuffix()
{
    if (!all_read && !isCancelled())
        throw Exception("readSuffix called before all data is read", ErrorCodes::LOGICAL_ERROR);

    finalize();

    for (auto & child : children)
        child->readSuffix();
}


void MergingAggregatedMemoryEfficientBlockInputStream::cancel(bool kill)
{
    if (kill)
        is_killed = true;

    bool old_val = false;
    if (!is_cancelled.compare_exchange_strong(old_val, true))
        return;

    if (parallel_merge_data)
    {
        {
            std::unique_lock lock(parallel_merge_data->merged_blocks_mutex);
            parallel_merge_data->finish = true;
        }
        parallel_merge_data->merged_blocks_changed.notify_one(); /// readImpl method must stop waiting and exit.
        parallel_merge_data->have_space.notify_all(); /// Merging threads must stop waiting and exit.
    }

    for (auto & input : inputs)
    {
        if (auto * child = dynamic_cast<IProfilingBlockInputStream *>(input.stream.get()))
        {
            try
            {
                child->cancel(kill);
            }
            catch (...)
            {
                /** If failed to ask to stop processing one or more sources.
                  * (example: connection reset during distributed query execution)
                  * - then don't care.
                  */
                LOG_ERROR(log, "Exception while cancelling {}", child->getName());
            }
        }
    }
}


void MergingAggregatedMemoryEfficientBlockInputStream::start()
{
    if (started)
        return;

    started = true;

    /// If child is RemoteBlockInputStream, then child->readPrefix() will send query to remote server, initiating calculations.

    if (reading_threads == 1)
    {
        for (auto & child : children)
            child->readPrefix();
    }
    else
    {
        size_t num_children = children.size();
        for (size_t i = 0; i < num_children; ++i)
        {
            auto & child = children[i];

            reading_pool->schedule(wrapInvocable(true, [&child] { child->readPrefix(); }));
        }

        reading_pool->wait();
    }

    if (merging_threads > 1)
    {
        /** Create threads that will receive and merge blocks.
          */

        for (size_t i = 0; i < merging_threads; ++i)
            parallel_merge_data->thread_pool->schedule(true, [this] { mergeThread(); });
    }
}


Block MergingAggregatedMemoryEfficientBlockInputStream::readImpl()
{
    start();

    if (Block block = popBlocksListFront(current_result))
    {
        return block;
    }

    if (!parallel_merge_data)
    {
        BlocksToMerge blocks_to_merge = getNextBlocksToMerge();
        if (blocks_to_merge && !blocks_to_merge->empty())
        {
            current_result = aggregator.vstackBlocks(*blocks_to_merge, final);
            auto block = popBlocksListFront(current_result);
            assert(block);
            return block;
        }
        else
        {
            /// if all the buckets are done, return empty block
            return {};
        }
    }
    else
    {
        Block res;

        while (true)
        {
            std::unique_lock lock(parallel_merge_data->merged_blocks_mutex);

            parallel_merge_data->merged_blocks_changed.wait(lock, [this] {
                return parallel_merge_data->finish /// Requested to finish early.
                    || parallel_merge_data->exception /// An error in merging thread.
                    || parallel_merge_data->exhausted /// No more data in sources.
                    || !parallel_merge_data->merged_blocks.empty(); /// Have another merged block.
            });

            if (parallel_merge_data->exception)
                std::rethrow_exception(parallel_merge_data->exception);

            if (parallel_merge_data->finish)
                break;

            bool have_merged_block_or_merging_in_progress = !parallel_merge_data->merged_blocks.empty();

            if (parallel_merge_data->exhausted && !have_merged_block_or_merging_in_progress)
                break;

            if (have_merged_block_or_merging_in_progress)
            {
                auto it = parallel_merge_data->merged_blocks.begin();

                if (!it->second.empty())
                {
                    current_result.swap(it->second);
                    parallel_merge_data->merged_blocks.erase(it);

                    lock.unlock();
                    parallel_merge_data->have_space
                        .notify_one(); /// We consumed block. Merging thread may merge next block for us.
                    res = popBlocksListFront(current_result);
                    assert(res);
                    break;
                }
            }
        }

        if (!res)
            all_read = true;

        return res;
    }
}


MergingAggregatedMemoryEfficientBlockInputStream::~MergingAggregatedMemoryEfficientBlockInputStream()
{
    try
    {
        if (!all_read)
            cancel(false);

        finalize();
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }
}


void MergingAggregatedMemoryEfficientBlockInputStream::finalize()
{
    if (!started)
        return;

    LOG_TRACE(log, "Waiting for threads to finish");

    if (parallel_merge_data)
        parallel_merge_data->thread_pool->wait();

    LOG_TRACE(log, "Waited for threads to finish");
}


void MergingAggregatedMemoryEfficientBlockInputStream::mergeThread()
{
    try
    {
        while (!parallel_merge_data->finish)
        {
            /** Receiving next blocks is processing by one thread pool, and merge is in another.
              * This is quite complex interaction.
              * Each time:
              * - 'reading_threads' will read one next block from each source;
              * - group of blocks for merge is created from them;
              * - one of 'merging_threads' will do merge this group of blocks;
              */
            BlocksToMerge blocks_to_merge;
            int output_order = -1;

            /** Synchronously:
              * - fetch next blocks from sources,
              *    wait for space in 'merged_blocks'
              *    and reserve a place in 'merged_blocks' to do merge of them;
              * - or, if no next blocks, set 'exhausted' flag.
              */
            {
                std::lock_guard lock(parallel_merge_data->get_next_blocks_mutex);

                if (parallel_merge_data->exhausted || parallel_merge_data->finish)
                    break;

                blocks_to_merge = getNextBlocksToMerge();

                if (!blocks_to_merge || blocks_to_merge->empty())
                {
                    {
                        std::unique_lock merged_blocks_lock(parallel_merge_data->merged_blocks_mutex);
                        parallel_merge_data->exhausted = true;
                    }

                    /// No new blocks has been read from sources. (But maybe, in another mergeThread, some previous block is still prepared.)
                    parallel_merge_data->merged_blocks_changed.notify_one();
                    break;
                }

                output_order = blocks_to_merge->front().info.bucket_num;

                {
                    std::unique_lock merged_blocks_lock(parallel_merge_data->merged_blocks_mutex);

                    parallel_merge_data->have_space.wait(merged_blocks_lock, [this] {
                        return parallel_merge_data->merged_blocks.size() < merging_threads
                            || parallel_merge_data->finish;
                    });

                    if (parallel_merge_data->finish)
                        break;

                    /** Place empty blockslist. It is promise to do merge and fill it.
                      * Main thread knows, that there will be result for 'output_order' place.
                      * Main thread must return results exactly in 'output_order', so that is important.
                      */
                    parallel_merge_data->merged_blocks[output_order];
                }
            }

            /// At this point, several merge threads may work in parallel.
            BlocksList res = aggregator.vstackBlocks(*blocks_to_merge, final);
            assert(!res.empty());

            {
                std::lock_guard lock(parallel_merge_data->merged_blocks_mutex);

                if (parallel_merge_data->finish)
                    break;
                parallel_merge_data->merged_blocks[output_order] = res;
            }

            /// Notify that we have another merged block.
            parallel_merge_data->merged_blocks_changed.notify_one();
        }
    }
    catch (...)
    {
        {
            std::lock_guard lock(parallel_merge_data->merged_blocks_mutex);
            parallel_merge_data->exception = std::current_exception();
            parallel_merge_data->finish = true;
        }

        parallel_merge_data->merged_blocks_changed.notify_one();
        parallel_merge_data->have_space.notify_all();
    }
}


MergingAggregatedMemoryEfficientBlockInputStream::BlocksToMerge MergingAggregatedMemoryEfficientBlockInputStream::
    getNextBlocksToMerge()
{
    /** There are several input sources.
      * From each of them, data may be received in one of following forms:
      *
      * 1. Block with specified 'bucket_num'.
      * It means, that on remote server, data was partitioned by buckets.
      * And data for each 'bucket_num' from different servers may be merged independently.
      * Because data in different buckets will contain different aggregation keys.
      * Data for different 'bucket_num's will be received in increasing order of 'bucket_num'.
      *
      * 2. Block without specified 'bucket_num'.
      * It means, that on remote server, data was not partitioned by buckets.
      * If all servers will send non-partitioned data, we may just merge it.
      * But if some other servers will send partitioned data,
      *  then we must first partition non-partitioned data, and then merge data in each partition.
      */
    ++current_bucket_num;

    /// Read from source next block with bucket number not greater than 'current_bucket_num'.

    auto need_that_input = [this](Input & input) {
        return !input.is_exhausted && input.block.info.bucket_num < current_bucket_num;
    };

    auto read_from_input = [this](Input & input) {
        while (true)
        {
            Block block = input.stream->read();

            if (!block)
            {
                input.is_exhausted = true;
                break;
            }

            if (block.info.bucket_num != -1)
            {
                /// One of partitioned blocks for two-level data.
                has_two_level = true;
                input.block = block;
            }
            else
            {
                /// Block for non-partitioned (single-level) data.
                input.block = block;
            }

            break;
        }
    };

    if (reading_threads == 1)
    {
        for (auto & input : inputs)
            if (need_that_input(input))
                read_from_input(input);
    }
    else
    {
        for (auto & input : inputs)
        {
            if (need_that_input(input))
            {
                reading_pool->schedule(wrapInvocable(true, [&input, &read_from_input] { read_from_input(input); }));
            }
        }

        reading_pool->wait();
    }

    while (true)
    {
        if (current_bucket_num >= NUM_BUCKETS)
        {
            return {};
        }
        else if (has_two_level)
        {
            /** Having two-level (partitioned) data.
              * Will process by bucket numbers in increasing order.
              * Find minimum bucket number, for which there is data
              *  - this will be data for merge.
              */
            int min_bucket_num = NUM_BUCKETS;

            for (auto & input : inputs)
            {
                /// Blocks for already partitioned (two-level) data.
                if (input.block.info.bucket_num != -1 && input.block.info.bucket_num < min_bucket_num)
                    min_bucket_num = input.block.info.bucket_num;

                /// Not yet partitioned (splitted to buckets) block. Will partition it and place result to 'splitted_blocks'.
                if (input.block.info.bucket_num == -1 && input.block && input.splitted_blocks.empty())
                {
                    LOG_TRACE(log, "Having block without bucket: will split.");

                    input.splitted_blocks = aggregator.convertBlockToTwoLevel(input.block);
                    input.block = Block();
                }

                /// Blocks we got by splitting non-partitioned blocks.
                if (!input.splitted_blocks.empty())
                {
                    for (const auto & block : input.splitted_blocks)
                    {
                        if (block && block.info.bucket_num < min_bucket_num)
                        {
                            min_bucket_num = block.info.bucket_num;
                            break;
                        }
                    }
                }
            }

            current_bucket_num = min_bucket_num;

            /// No more blocks with ordinary data.
            if (current_bucket_num == NUM_BUCKETS)
                continue;

            /// Collect all blocks for 'current_bucket_num' to do merge.
            BlocksToMerge blocks_to_merge = std::make_unique<BlocksList>();

            for (auto & input : inputs)
            {
                if (input.block.info.bucket_num == current_bucket_num)
                {
                    blocks_to_merge->emplace_back(std::move(input.block));
                    input.block = Block();
                }
                else if (!input.splitted_blocks.empty() && input.splitted_blocks[min_bucket_num])
                {
                    blocks_to_merge->emplace_back(std::move(input.splitted_blocks[min_bucket_num]));
                    input.splitted_blocks[min_bucket_num] = Block();
                }
            }

            return blocks_to_merge;
        }
        else
        {
            /// There are only non-partitioned (single-level) data. Just merge them.

            BlocksToMerge blocks_to_merge = std::make_unique<BlocksList>();

            for (auto & input : inputs)
                if (input.block)
                    blocks_to_merge->emplace_back(std::move(input.block));

            current_bucket_num = NUM_BUCKETS;
            return blocks_to_merge;
        }
    }
}

} // namespace DB
