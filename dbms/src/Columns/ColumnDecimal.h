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

#include <Columns/ColumnVectorHelper.h>
#include <Columns/IColumn.h>
#include <Common/typeid_cast.h>

#include <cmath>


namespace DB
{
namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
}

/// PaddedPODArray extended by Decimal scale
template <typename T>
class DecimalPaddedPODArray : public PaddedPODArray<T>
{
public:
    using Base = PaddedPODArray<T>;
    using Base::operator[];

    DecimalPaddedPODArray(size_t size, UInt32 scale_)
        : Base(size)
        , scale(scale_)
    {}

    DecimalPaddedPODArray(size_t size, const T & x, UInt32 scale_)
        : Base(size, x)
        , scale(scale_)
    {}

    DecimalPaddedPODArray(const DecimalPaddedPODArray & other)
        : Base(other.begin(), other.end())
        , scale(other.scale)
    {}

    DecimalPaddedPODArray(DecimalPaddedPODArray && other)
    {
        this->swap(other);
        std::swap(scale, other.scale);
    }

    DecimalPaddedPODArray & operator=(DecimalPaddedPODArray && other)
    {
        this->swap(other);
        std::swap(scale, other.scale);
        return *this;
    }

    UInt32 getScale() const { return scale; }

private:
    UInt32 scale;
};


/// A ColumnVector for Decimals
template <typename T>
class ColumnDecimal final : public COWPtrHelper<ColumnVectorHelper, ColumnDecimal<T>>
{
    static_assert(IsDecimal<T>);

private:
    using Self = ColumnDecimal;
    friend class COWPtrHelper<ColumnVectorHelper, Self>;

public:
    using value_type = T;
    using Container = DecimalPaddedPODArray<T>;

private:
    static constexpr bool is_Decimal256 = std::is_same_v<Decimal256, T>;
    ColumnDecimal(const size_t n, UInt32 scale_)
        : data(n, scale_)
        , scale(scale_)
    {}
    ColumnDecimal(const size_t n, const T & x, UInt32 scale_)
        : data(n, x, scale_)
        , scale(scale_)
    {}

    ColumnDecimal(const ColumnDecimal & src)
        : data(src.data)
        , scale(src.scale)
    {}

public:
    const char * getFamilyName() const override { return TypeName<T>::get(); }

    bool isNumeric() const override { return false; }
    bool canBeInsideNullable() const override { return true; }
    bool isFixedAndContiguous() const override { return true; }
    size_t sizeOfValueIfFixed() const override { return sizeof(T); }

    size_t size() const override { return data.size(); }
    size_t byteSize() const override { return data.size() * sizeof(data[0]); }
    size_t byteSize(size_t /*offset*/, size_t limit) const override { return limit * sizeof(data[0]); }
    size_t allocatedBytes() const override { return data.allocated_bytes(); }
    //void protect() override { data.protect(); }
    void reserve(size_t n) override { data.reserve(n); }
    void reserveAlign(size_t n, size_t alignment) override { data.reserve(n, alignment); }

    void insertFrom(const IColumn & src, size_t n) override
    {
        data.push_back(static_cast<const Self &>(src).getData()[n]);
    }
    void insertData(const char * src, size_t /*length*/) override;
    bool decodeTiDBRowV2Datum(size_t cursor, const String & raw_value, size_t length, bool force_decode) override;
    void insertDefault() override { data.push_back(T()); }
    void insertManyDefaults(size_t length) override { data.resize_fill(data.size() + length, T()); }
    void insert(const Field & x) override { data.push_back(DB::get<typename NearestFieldType<T>::Type>(x)); }
    void insertRangeFrom(const IColumn & src, size_t start, size_t length) override;
    void insertManyFrom(const IColumn & src_, size_t position, size_t length) override;
    void insertSelectiveRangeFrom(
        const IColumn & src_,
        const IColumn::Offsets & selective_offsets,
        size_t start,
        size_t length) override;
    void popBack(size_t n) override { data.resize_assume_reserved(data.size() - n); }

    StringRef getRawData() const override
    {
        if constexpr (is_Decimal256)
        {
            throw Exception("getRawData is not supported for " + IColumn::getName());
        }
        return StringRef(reinterpret_cast<const char *>(data.data()), byteSize());
    }

    StringRef serializeValueIntoArena(
        size_t n,
        Arena & arena,
        char const *& begin,
        const TiDB::TiDBCollatorPtr &,
        String &) const override;
    const char * deserializeAndInsertFromArena(const char * pos, const TiDB::TiDBCollatorPtr &) override;

    size_t serializeByteSize() const override { return data.size() * sizeof(T); }

    void countSerializeByteSize(PaddedPODArray<size_t> & byte_size) const override;
    void countSerializeByteSizeForCmp(
        PaddedPODArray<size_t> & byte_size,
        const NullMap * /* nullmap */,
        const TiDB::TiDBCollatorPtr &) const override
    {
        countSerializeByteSize(byte_size);
    }

    void countSerializeByteSizeForColumnArray(
        PaddedPODArray<size_t> & byte_size,
        const IColumn::Offsets & array_offsets) const override;
    void countSerializeByteSizeForCmpColumnArray(
        PaddedPODArray<size_t> & byte_size,
        const IColumn::Offsets & array_offsets,
        const NullMap * nullmap,
        const TiDB::TiDBCollatorPtr &) const override;
    template <bool has_nullmap>
    void countSerializeByteSizeForColumnArrayImpl(
        PaddedPODArray<size_t> & byte_size,
        const IColumn::Offsets & array_offsets,
        const NullMap * nullmap) const;

    void serializeToPos(PaddedPODArray<char *> & pos, size_t start, size_t length, bool has_null) const override;
    void serializeToPosForCmp(
        PaddedPODArray<char *> & pos,
        size_t start,
        size_t length,
        bool has_null,
        const NullMap * nullmap,
        const TiDB::TiDBCollatorPtr &,
        String *) const override;
    template <bool has_null, bool compare_semantics, bool has_nullmap>
    void serializeToPosImpl(PaddedPODArray<char *> & pos, size_t start, size_t length, const NullMap * nullmap) const;

    void serializeToPosForColumnArray(
        PaddedPODArray<char *> & pos,
        size_t start,
        size_t length,
        bool has_null,
        const IColumn::Offsets & array_offsets) const override;
    void serializeToPosForCmpColumnArray(
        PaddedPODArray<char *> & pos,
        size_t start,
        size_t length,
        bool has_null,
        const NullMap * nullmap,
        const IColumn::Offsets & array_offsets,
        const TiDB::TiDBCollatorPtr &,
        String *) const override;
    template <bool has_null, bool compare_semantics, bool has_nullmap>
    void serializeToPosForColumnArrayImpl(
        PaddedPODArray<char *> & pos,
        size_t start,
        size_t length,
        const IColumn::Offsets & array_offsets,
        const NullMap * nullmap) const;

    void deserializeAndInsertFromPos(PaddedPODArray<char *> & pos, bool use_nt_align_buffer) override;

    void deserializeAndInsertFromPosForColumnArray(
        PaddedPODArray<char *> & pos,
        const IColumn::Offsets & array_offsets,
        bool use_nt_align_buffer) override;

    void flushNTAlignBuffer() override;

    void deserializeAndAdvancePos(PaddedPODArray<char *> & pos) const override
    {
        IColumn::advancePosByOffset(pos, sizeof(T));
    }

    void deserializeAndAdvancePosForColumnArray(PaddedPODArray<char *> & pos, const IColumn::Offsets & array_offsets)
        const override;

    void updateHashWithValue(size_t n, SipHash & hash, const TiDB::TiDBCollatorPtr &, String &) const override;
    void updateHashWithValues(IColumn::HashValues & hash_values, const TiDB::TiDBCollatorPtr &, String &)
        const override;
    void updateWeakHash32(WeakHash32 & hash, const TiDB::TiDBCollatorPtr &, String &) const override;
    void updateWeakHash32(WeakHash32 & hash, const TiDB::TiDBCollatorPtr &, String &, const BlockSelective & selective)
        const override;
    int compareAt(size_t n, size_t m, const IColumn & rhs_, int nan_direction_hint) const override;
    void getPermutation(bool reverse, size_t limit, int nan_direction_hint, IColumn::Permutation & res) const override;

    MutableColumnPtr cloneResized(size_t size) const override;

    Field operator[](size_t n) const override { return DecimalField(data[n], scale); }

    //StringRef getRawData() const override { return StringRef(reinterpret_cast<const char*>(data.data()), data.size()); }
    StringRef getDataAt(size_t n) const override
    {
        if constexpr (is_Decimal256)
        {
            throw Exception("getDataAt is not supported for " + IColumn::getName());
        }
        return StringRef(reinterpret_cast<const char *>(&data[n]), sizeof(data[n]));
    }
    void get(size_t n, Field & res) const override { res = (*this)[n]; }
    //    bool getBool(size_t n) const override { return bool(data[n]); }
    Int64 getInt(size_t n) const override { return Int64(static_cast<typename T::NativeType>(data[n]) * scale); }
    UInt64 get64(size_t n) const override;
    //    bool isDefaultAt(size_t n) const override { return data[n] == 0; }

    ColumnPtr filter(const IColumn::Filter & filt, ssize_t result_size_hint) const override;
    ColumnPtr permute(const IColumn::Permutation & perm, size_t limit) const override;
    //ColumnPtr index(const IColumn & indexes, size_t limit) const override;

    template <typename Type>
    ColumnPtr indexImpl(const PaddedPODArray<Type> & indexes, size_t limit) const;

    ColumnPtr replicateRange(size_t start_row, size_t end_row, const IColumn::Offsets & offsets) const override;

    void getExtremes(Field & min, Field & max) const override;

    MutableColumns scatter(IColumn::ColumnIndex num_columns, const IColumn::Selector & selector) const override
    {
        return this->template scatterImpl<Self>(num_columns, selector);
    }

    MutableColumns scatter(
        IColumn::ColumnIndex num_columns,
        const IColumn::Selector & selector,
        const BlockSelective & selective) const override
    {
        return this->template scatterImpl<Self>(num_columns, selector, selective);
    }

    void scatterTo(IColumn::ScatterColumns & columns, const IColumn::Selector & selector) const override
    {
        return this->template scatterToImpl<Self>(columns, selector);
    }
    void scatterTo(
        IColumn::ScatterColumns & columns,
        const IColumn::Selector & selector,
        const BlockSelective & selective) const override
    {
        return this->template scatterToImpl<Self>(columns, selector, selective);
    }

    void gather(ColumnGathererStream & gatherer_stream) override;

    //bool structureEquals(const IColumn & rhs) const override
    //{
    //    if (auto rhs_concrete = typeid_cast<const ColumnDecimal<T> *>(&rhs))
    //        return scale == rhs_concrete->scale;
    //    return false;
    //}


    void insert(const T value) { data.push_back(value); }
    Container & getData() { return data; }
    const Container & getData() const { return data; }
    const T & getElement(size_t n) const { return data[n]; }
    T & getElement(size_t n) { return data[n]; }

    UInt32 getScale() const { return scale; }

protected:
    Container data;
    UInt32 scale;
    std::unique_ptr<ColumnNTAlignBufferAVX2> align_buffer_ptr;

    template <typename U>
    void permutation(bool reverse, size_t limit, PaddedPODArray<U> & res) const
    {
        size_t s = data.size();
        res.resize(s);
        for (U i = 0; i < s; ++i)
            res[i] = i;

        auto sort_end = res.end();
        if (limit && limit < s)
            sort_end = res.begin() + limit;

        if (reverse)
            std::partial_sort(res.begin(), sort_end, res.end(), [this](size_t a, size_t b) {
                return data[a].value > data[b].value;
            });
        else
            std::partial_sort(res.begin(), sort_end, res.end(), [this](size_t a, size_t b) {
                return data[a].value < data[b].value;
            });
    }

    template <bool selective_block>
    void updateWeakHash32Impl(WeakHash32 & hash, const BlockSelective & selective) const;
};

template <typename T>
template <typename Type>
ColumnPtr ColumnDecimal<T>::indexImpl(const PaddedPODArray<Type> & indexes, size_t limit) const
{
    size_t size = indexes.size();

    if (limit == 0)
        limit = size;
    else
        limit = std::min(size, limit);

    auto res = this->create(limit, scale);
    typename Self::Container & res_data = res->getData();
    for (size_t i = 0; i < limit; ++i)
        res_data[i] = data[indexes[i]];

    return std::move(res);
}

} // namespace DB
