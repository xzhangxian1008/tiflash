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

#pragma once

#include <Common/Allocator.h>
#include <Common/Exception.h>
#include <Common/ProfileEvents.h>
#include <Core/Defines.h>
#include <common/likely.h>
#include <string.h>

#include <boost/noncopyable.hpp>
#include <memory>
#include <vector>

namespace DB
{
using ResizeCallback = std::function<bool()>;
/** Memory pool to append something. For example, short strings.
  * Usage scenario:
  * - put lot of strings inside pool, keep their addresses;
  * - addresses remain valid during lifetime of pool;
  * - at destruction of pool, all memory is freed;
  * - memory is allocated and freed by large chunks;
  * - freeing parts of data is not possible (but look at ArenaWithFreeLists if you need);
  */
class Arena : private boost::noncopyable
{
private:
    /// Contiguous chunk of memory and pointer to free space inside it. Member of single-linked list.
    struct Chunk : private Allocator<false> /// empty base optimization
    {
        char * begin;
        char * pos;
        char * end;

        Chunk * prev;

        Chunk(size_t size_, Chunk * prev_)
        {
            begin = reinterpret_cast<char *>(Allocator::alloc(size_));
            pos = begin;
            end = begin + size_;
            prev = prev_;
        }

        ~Chunk()
        {
            Allocator::free(begin, size());
            delete prev;
        }

        size_t size() const { return end - begin; }
        size_t remaining() const { return end - pos; }
    };

    size_t growth_factor;
    size_t linear_growth_threshold;

    /// Last contiguous chunk of memory.
    Chunk * head;
    size_t size_in_bytes;

    ResizeCallback resize_callback;

    static size_t roundUpToPageSize(size_t s) { return (s + 4096 - 1) / 4096 * 4096; }

    /// If chunks size is less than 'linear_growth_threshold', then use exponential growth, otherwise - linear growth
    ///  (to not allocate too much excessive memory).
    size_t nextSize(size_t min_next_size) const
    {
        size_t size_after_grow = 0;

        if (head->size() < linear_growth_threshold)
            size_after_grow = head->size() * growth_factor;
        else
            size_after_grow = linear_growth_threshold;

        if (size_after_grow < min_next_size)
            size_after_grow = min_next_size;

        return roundUpToPageSize(size_after_grow);
    }

    /// Add next contiguous chunk of memory with size not less than specified.
    /// If free_empty_head_chunk is true, empty head will be freed.
    /// It can avoid mem leak when you want always reuse one chunk.
    void NO_INLINE addChunk(size_t min_size, bool free_empty_head_chunk)
    {
        if (resize_callback != nullptr)
        {
            if unlikely (!resize_callback())
                throw ResizeException("Error in arena resize");
        }
        const auto next_size = nextSize(min_size);
        if (free_empty_head_chunk && head->remaining() == head->size())
        {
            size_in_bytes -= head->size();
            auto * old_head = head;
            head = head->prev;
            old_head->prev = nullptr;
            delete old_head;
        }
        head = new Chunk(next_size, head);
        size_in_bytes += head->size();
    }

    friend class ArenaAllocator;

public:
    explicit Arena(
        size_t initial_size_ = 4096,
        size_t growth_factor_ = 2,
        size_t linear_growth_threshold_ = 128 * 1024 * 1024)
        : growth_factor(growth_factor_)
        , linear_growth_threshold(linear_growth_threshold_)
        , head(new Chunk(initial_size_, nullptr))
        , size_in_bytes(head->size())
    {}

    ~Arena() { delete head; }

    /// Get piece of memory with alignment
    char * alignedAlloc(size_t size, size_t alignment, bool free_empty_head_chunk = false)
    {
        do
        {
            void * head_pos = head->pos;
            size_t space = head->end - head->pos;

            auto * res = static_cast<char *>(std::align(alignment, size, head_pos, space));
            if (res)
            {
                head->pos = static_cast<char *>(head_pos);
                head->pos += size;
                return res;
            }

            addChunk(size + alignment, free_empty_head_chunk);
        } while (true);
    }

    /// Get piece of memory, without alignment.
    char * alloc(size_t size, bool free_empty_head_chunk = false)
    {
        if (unlikely(head->pos + size > head->end))
            addChunk(size, free_empty_head_chunk);

        char * res = head->pos;
        head->pos += size;
        return res;
    }

    /** Rollback just performed allocation.
      * Must pass size not more that was just allocated.
      */
    void rollback(size_t size) { head->pos -= size; }
    void rollback() { head->pos = head->begin; }

    void setResizeCallback(const ResizeCallback & resize_callback_) { resize_callback = resize_callback_; }

    /** Begin or expand allocation of contiguous piece of memory.
      * 'begin' - current begin of piece of memory, if it need to be expanded, or nullptr, if it need to be started.
      * If there is no space in chunk to expand current piece of memory - then copy all piece to new chunk and change value of 'begin'.
      * NOTE This method is usable only for latest allocation. For earlier allocations, see 'realloc' method.
      */
    char * allocContinue(size_t size, char const *& begin)
    {
        while (unlikely(head->pos + size > head->end))
        {
            char * prev_end = head->pos;
            addChunk(size, false);

            if (begin)
                begin = insert(begin, prev_end - begin);
            else
                break;
        }

        char * res = head->pos;
        head->pos += size;

        if (!begin)
            begin = res;

        return res;
    }

    /// NOTE Old memory region is wasted.
    char * realloc(const char * old_data, size_t old_size, size_t new_size)
    {
        char * res = alloc(new_size);
        if (old_data)
            memcpy(res, old_data, old_size);
        return res;
    }

    /// Insert string without alignment.
    const char * insert(const char * data, size_t size)
    {
        char * res = alloc(size);
        memcpy(res, data, size);
        return res;
    }

    /// Size of chunks in bytes.
    size_t size() const { return size_in_bytes; }

    size_t remainingSpaceInCurrentChunk() const { return head->remaining(); }
};

using ArenaPtr = std::shared_ptr<Arena>;
using Arenas = std::vector<ArenaPtr>;


} // namespace DB
