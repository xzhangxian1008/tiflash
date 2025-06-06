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

#include <Storages/DeltaMerge/Filter/RSOperator.h>

namespace DB::DM
{

class Not : public LogicalOp
{
public:
    explicit Not(const RSOperatorPtr & child)
        : LogicalOp({child})
    {}

    String name() override { return "not"; }

    RSResults roughCheck(size_t start_pack, size_t pack_count, const RSCheckParam & param) override
    {
        auto results = children[0]->roughCheck(start_pack, pack_count, param);
        std::transform(results.begin(), results.end(), results.begin(), [](const auto result) { return !result; });
        return results;
    }

    ColumnRangePtr buildSets(const google::protobuf::RepeatedPtrField<tipb::ColumnarIndexInfo> & index_infos) override
    {
        auto sets = children[0]->buildSets(index_infos);
        return sets->invert();
    }
};

} // namespace DB::DM
