// Copyright 2022 PingCAP, Ltd.
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

#include <common/types.h>
#include <fmt/format.h>

namespace DB::DM::Remote
{

/**
 * Intra-cluster unique ID for a DMFile.
 */
struct DMFileOID // TODO: Should be generated by a protobuf.
{
    UInt64 write_node_id = 0;
    Int64 table_id = 0;
    UInt64 file_id = 0;

    String info() const
    {
        return fmt::format("<node={} table={} file={}>", write_node_id, table_id, file_id);
    }
};

/**
 * Intra-cluster unique ID for a page.
 */
struct PageOID // TODO: Should be generated by a protobuf.
{
    UInt64 write_node_id = 0;
    Int64 table_id = 0;
    UInt64 page_id = 0;
    // TODO: distinguish log / meta / data?

    String info() const
    {
        return fmt::format("<node={} table={} page={}>", write_node_id, table_id, page_id);
    }
};

} // namespace DB::DM::Remote