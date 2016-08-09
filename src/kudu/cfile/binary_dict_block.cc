// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "kudu/cfile/binary_dict_block.h"

#include <glog/logging.h>
#include <algorithm>
#include <unordered_set>
#include <set>

#include "kudu/cfile/sorted_plain_block.h"
#include "kudu/cfile/cfile_reader.h"
#include "kudu/cfile/cfile_util.h"
#include "kudu/cfile/cfile_writer.h"
#include "kudu/cfile/bshuf_block.h"
#include "kudu/common/columnblock.h"
#include "kudu/gutil/casts.h"
#include "kudu/gutil/stringprintf.h"
#include "kudu/util/coding.h"
#include "kudu/util/coding-inl.h"
#include "kudu/util/group_varint-inl.h"
#include "kudu/util/hexdump.h"
#include "kudu/util/memory/arena.h"

namespace kudu {
namespace cfile {

BinaryDictBlockBuilder::BinaryDictBlockBuilder(const WriterOptions* options)
  : options_(options),
    dict_block_(options_),
    dictionary_strings_arena_(1024, 32*1024*1024),
    mode_(kCodeWordMode) {
  sort_builder_.reset(new BShufBlockBuilder<UINT32>(options_));
  data_builder_.reset(new BShufBlockBuilder<UINT32>(options_));
  Reset();
}

void BinaryDictBlockBuilder::Reset() {
  buffer_.clear();
  buffer_.resize(kMaxHeaderSize);
  buffer_.reserve(options_->storage_attributes.cfile_block_size);

  if (mode_ == kCodeWordMode &&
      dict_block_.IsBlockFull(options_->storage_attributes.cfile_block_size)) {
    mode_ = kPlainBinaryMode;
//    data_builder_.reset(new BinaryPlainBlockBuilder(options_));
    data_builder_.reset(new SortedPlainBlockBuilder(options_));
    sort_builder_.reset();
  } else {
    data_builder_->Reset();
    sort_builder_->Reset();
  }
  finished_ = false;
}

Slice BinaryDictBlockBuilder::Finish(rowid_t ordinal_pos) {
  finished_ = true;

  InlineEncodeFixed32(&buffer_[0], mode_);
  // TODO: if we could modify the the Finish() API a little bit, we can
  // avoid an extra memory copy (buffer_.append(..))
  Slice data_slice = data_builder_->Finish(ordinal_pos);
  buffer_.append(data_slice.data(), data_slice.size());

  return Slice(buffer_);
}

// The current block is considered full when the the size of data block
// exceeds limit or when the size of dictionary block exceeds the
// CFile block size.
//
// If it is the latter case, all the subsequent data blocks will switch to
// StringPlainBlock automatically.
bool BinaryDictBlockBuilder::IsBlockFull(size_t limit) const {
  int block_size = options_->storage_attributes.cfile_block_size;
  if (data_builder_->IsBlockFull(block_size)) return true;
  if (dict_block_.IsBlockFull(block_size) && (mode_ == kCodeWordMode)) return true;
  return false;
}

int BinaryDictBlockBuilder::AddCodeWords(const uint8_t* vals, size_t count) {
  DCHECK(!finished_);
  DCHECK_GT(count, 0);
  size_t i;
  if (data_builder_->Count() == 0) {
    const Slice* first = reinterpret_cast<const Slice*>(vals);
    first_key_.assign_copy(first->data(), first->size());
  }
  for (i = 0; i < count; i++) {
    uint32_t codeword = 0;
    if (dict_block_.Add(vals, 1) == 0) {
      break;
    }
    const Slice* src = reinterpret_cast<const Slice*>(vals);
    const char* c_str = reinterpret_cast<const char*>(src->data());
    StringPiece current_item(c_str, src->size());
    codeword = dict_block_.CodewordOf(current_item);
    // The data block is full
    if (data_builder_->Add(reinterpret_cast<const uint8_t*>(&codeword), 1) == 0) {
      break;
    }
    // Keep the codewords sorted in memory
    vals += sizeof(Slice);
  }
  return i;
}

int BinaryDictBlockBuilder::Add(const uint8_t* vals, size_t count) {
  if (mode_ == kCodeWordMode) {
    return AddCodeWords(vals, count);
  } else {
    DCHECK_EQ(mode_, kPlainBinaryMode);
    return data_builder_->Add(vals, count);
  }
}

Status BinaryDictBlockBuilder::AppendExtraInfo(CFileWriter* c_writer, CFileFooterPB* footer) {
  Slice dict_slice = dict_block_.Finish(0);

  std::vector<Slice> dict_v;
  dict_v.push_back(dict_slice);

  BlockPointer ptr;
  Status s = c_writer->AppendDictBlock(dict_v, &ptr, "Append dictionary block");
  if (!s.ok()) {
    LOG(WARNING) << "Unable to append block to file: " << s.ToString();
    return s;
  }
  ptr.CopyToPB(footer->mutable_dict_block_ptr());
  return Status::OK();
}

size_t BinaryDictBlockBuilder::Count() const {
  return data_builder_->Count();
}

Status BinaryDictBlockBuilder::GetFirstKey(void* key_void) const {
  if (mode_ == kCodeWordMode) {
    CHECK(finished_);
    Slice* slice = reinterpret_cast<Slice*>(key_void);
    *slice = Slice(first_key_);
    return Status::OK();
  } else {
    DCHECK_EQ(mode_, kPlainBinaryMode);
    return data_builder_->GetFirstKey(key_void);
  }
}

////////////////////////////////////////////////////////////
// Decoding
////////////////////////////////////////////////////////////

BinaryDictBlockDecoder::BinaryDictBlockDecoder(Slice slice, CFileIterator* iter)
    : data_(std::move(slice)),
      parsed_(false) {
  dict_decoder_ = iter->GetDictDecoder();
  upper_rank_ = iter->GetDictPredicate()->upper_rank;
  lower_rank_ = iter->GetDictPredicate()->lower_rank;
}

Status BinaryDictBlockDecoder::ParseHeader() {
  CHECK(!parsed_);

  if (data_.size() < kMinHeaderSize) {
    return Status::Corruption(
      strings::Substitute("not enough bytes for header: dictionary block header "
        "size ($0) less than minimum possible header length ($1)",
        data_.size(), kMinHeaderSize));
  }

  bool valid = tight_enum_test_cast<DictEncodingMode>(DecodeFixed32(&data_[0]), &mode_);
  if (PREDICT_FALSE(!valid)) {
    return Status::Corruption("header Mode information corrupted");
  }

  Slice content(data_.data() + 4, data_.size() - 4);
  if (mode_ == kCodeWordMode) {
    data_decoder_.reset(new BShufBlockDecoder<UINT32>(content));
  } else {
    if (mode_ != kPlainBinaryMode) {
      return Status::Corruption("Unrecognized Dictionary encoded data block header");
    }
    data_decoder_.reset(new BinaryPlainBlockDecoder(content));
  }
  RETURN_NOT_OK(data_decoder_->ParseHeader());
  parsed_ = true;
  return Status::OK();
}

void BinaryDictBlockDecoder::SeekToPositionInBlock(uint pos) {
  data_decoder_->SeekToPositionInBlock(pos);
}

// value_void is the string word
Status BinaryDictBlockDecoder::SeekAtOrAfterValue(const void* value_void, bool* exact) {
  if (mode_ == kCodeWordMode) {
    DCHECK(value_void != nullptr);
    Status s = dict_decoder_->SeekAtOrAfterValue(value_void, exact);
    if (!s.ok()) {
      // This case means the value_void is larger that the largest key
      // in the dictionary block. Therefore, it is impossible to be in
      // the current data block, and we adjust the index to be the end
      // of the block
      data_decoder_->SeekToPositionInBlock(data_decoder_->Count() - 1);
      return s;
    }

    size_t index = dict_decoder_->GetCurrentIndex();
    bool tmp;
    return data_decoder_->SeekAtOrAfterValue(&index, &tmp);
  } else {
    DCHECK_EQ(mode_, kPlainBinaryMode);
    return data_decoder_->SeekAtOrAfterValue(value_void, exact);
  }
}

Status BinaryDictBlockDecoder::EvaluatePredicate(ColumnEvalContext *ctx,
                                                 size_t& offset,
                                                 size_t& n,
                                                 ColumnDataView* dst) {
  switch (ctx->pred().predicate_type()) {
    case PredicateType::None:
      ctx->eval_complete() = true;
      return Status::OK();
    case PredicateType::IsNotNull:
      ctx->eval_complete() = false;
      return Status::OK();
    case PredicateType::Range:
    case PredicateType::Equality:
      ctx->eval_complete() = true;
      break;
  }
  // ASSERT_NOT_NULL(bounds)

  BShufBlockDecoder<UINT32>* d_bptr = down_cast<BShufBlockDecoder<UINT32>*>(data_decoder_.get());

  // Copy the words of the data block into a buffer so that we can easily access the UINT32s
  // Load the rows' codeword values into a buffer for scanning
  codeword_buf_.resize(n*sizeof(uint32_t));
  d_bptr->CopyNextValuesToArray(&n, codeword_buf_.data());

  // Iterate through the data and check which satisfy the predicate
  // Regardless of whether it satisfies, put it to the output buffer
  Slice* out = reinterpret_cast<Slice*>(dst->data());
  Arena* out_arena = dst->arena();
  for (size_t i = 0; i < n; i++) {
    uint32_t codeword = *reinterpret_cast<uint32_t*>(&codeword_buf_[i*sizeof(uint32_t)]);
    
    // CopyNextDecodeStrings, append the string to out_arena with index out
    Slice elem = dict_decoder_->string_at_index(codeword);
    CHECK(out_arena->RelocateSlice(elem, out));
    out++;

    if (ctx->pred().predicate_type() == PredicateType ::Equality && dict_decoder_->RankOfCodeword(codeword)) {
      BitmapSet(ctx->sel()->mutable_bitmap(), offset+i);
    }
    else if (dict_decoder_->RankOfCodeword(codeword) >= lower_rank_ && dict_decoder_->RankOfCodeword(codeword) < upper_rank_) {
      BitmapSet(ctx->sel()->mutable_bitmap(), offset+i);
    }
    else {

    }
  }
  offset += n;
  return Status::OK();
}

Status BinaryDictBlockDecoder::CopyNextDecodeStrings(size_t* n, ColumnDataView* dst) {
  DCHECK(parsed_);
  CHECK_EQ(dst->type_info()->physical_type(), BINARY);
  DCHECK_LE(*n, dst->nrows());
  DCHECK_EQ(dst->stride(), sizeof(Slice));

  Arena* out_arena = dst->arena();
  Slice* out = reinterpret_cast<Slice*>(dst->data());

  codeword_buf_.resize((*n)*sizeof(uint32_t));

  // Copy the codewords into a temporary buffer first.
  // And then Copy the strings corresponding to the codewords to the destination buffer.
  BShufBlockDecoder<UINT32>* d_bptr = down_cast<BShufBlockDecoder<UINT32>*>(data_decoder_.get());
  RETURN_NOT_OK(d_bptr->CopyNextValuesToArray(n, codeword_buf_.data()));

  for (int i = 0; i < *n; i++) {
    uint32_t codeword = *reinterpret_cast<uint32_t*>(&codeword_buf_[i*sizeof(uint32_t)]);
    Slice elem = dict_decoder_->string_at_index(codeword);
    CHECK(out_arena->RelocateSlice(elem, out));
    out++;
  }
  return Status::OK();
}

Status BinaryDictBlockDecoder::CopyNextValues(size_t* n, ColumnDataView* dst) {
  if (mode_ == kCodeWordMode) {
    return CopyNextDecodeStrings(n, dst);
  } else {
    DCHECK_EQ(mode_, kPlainBinaryMode);
    return data_decoder_->CopyNextValues(n, dst);
  }
}

} // namespace cfile
} // namespace kudu
