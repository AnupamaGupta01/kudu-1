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
    data_builder_.reset(new BinaryPlainBlockBuilder(options_));
  } else {
    data_builder_->Reset();
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
    const Slice* src = reinterpret_cast<const Slice*>(vals);
    const char* c_str = reinterpret_cast<const char*>(src->data());
    StringPiece current_item(c_str, src->size());
    uint32_t codeword;

    if (!FindCopy(dictionary_, current_item, &codeword)) {
      // The dictionary block is full
      if (dict_block_.Add(vals, 1) == 0) {
        break;
      }
      const uint8_t* s_ptr = dictionary_strings_arena_.AddSlice(*src);
      if (s_ptr == nullptr) {
        // Arena does not have enough space for string content
        // Ideally, it should not happen.
        LOG(ERROR) << "Arena of Dictionary Encoder does not have enough memory for strings";
        break;
      }
      const char* s_content = reinterpret_cast<const char*>(s_ptr);
      codeword = dict_block_.Count() - 1;
      InsertOrDie(&dictionary_, StringPiece(s_content, src->size()), codeword);
    }
    // The data block is full
    if (data_builder_->Add(reinterpret_cast<const uint8_t*>(&codeword), 1) == 0) {
      break;
    }
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
    // SeekAtOrAfterValue doesn't make sense if the dict_decoder_ is not sorted
    // 
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

static bool CompareRange(Slice& str, Slice& lower, Slice& upper) {
  // will return true only if str is between upper and lower
  if (lower.compare(upper) == 0) {
    // this is an equality predicate
    return str.compare(lower) == 0;
  }
  if (upper.empty()) {
    // this is a lower bound
    return str.compare(lower) >= 0;
  }
  else if (lower.empty()) {
    // this is an upper bound
    return str.compare(upper) < 0;
  }
  else {
    // both bounds exist, get stuff between them
    return str.compare(upper) < 0 && str.compare(lower) >= 0;
  }
}

Status BinaryDictBlockDecoder::EvaluatePredicate(ColumnPredicate& pred,
                                                 SelectionVector *sel,
                                                 size_t& offset,
                                                 bool& eval_complete) {
  // start empty and scan for the correct values
  // offset is the offset in the selectionvector that we're currently looking at, since there will be
  //  multiple blocks
  // sel->SetAllFalse();  // SetAllFalse should be called before looping through all these blocks
  //   This should not happen per EvaluatePredicate call since there may be multiple bocks per batch
  std::unordered_set<uint32_t> pred_codewords;
  Slice lower, upper;
  if (pred.raw_lower() != nullptr) {
    lower = Slice(static_cast<const char*>(pred.raw_lower()));
  }
  else {
    lower = Slice();
  }
  if (pred.raw_upper() != nullptr) {
    upper = Slice(static_cast<const char*>(pred.raw_upper()));
  }
  else {
    upper = Slice();
  }

  switch (pred.predicate_type()) {
    case PredicateType::None: {
      eval_complete = true;
      return Status::OK();
    }
    case PredicateType::Range: {
      eval_complete = true;
      break;
    }
    case PredicateType::Equality: {
      eval_complete = true;
      upper = lower;
      break;
    }
    case PredicateType::IsNotNull: {
      eval_complete = false;
      return Status::OK();
    }
  }
  if (pred_codewords.empty()) {
    eval_complete = true;
    return Status::OK(); 
  }

  for (uint32_t i = 0; i < dict_decoder_->Count(); i++) {
    // Scan through the rows in the decoder and determine which satisfy the predicate
    Slice cur_string = dict_decoder_->string_at_index(i);

    // Store the codewords that satisfy the predicate to some storage
    if (CompareRange(cur_string, lower, upper)) {
      pred_codewords.insert(i);
    }
  }

  size_t nrows = data_decoder_->Count();
  
  BShufBlockDecoder<UINT32>* d_bptr = down_cast<BShufBlockDecoder<UINT32>*>(data_decoder_.get());
  d_bptr->SeekToPositionInBlock(0);
  d_bptr->CopyNextValuesToArray(&nrows, codeword_buf_.data());
  
  codeword_buf_.resize(nrows*sizeof(uint32_t));
  
  // iterate through the data and check if the which satisfy the predicate
  for (size_t i = 0; i < nrows; i++) {
    uint32_t codeword = *reinterpret_cast<uint32_t*>(&codeword_buf_[i*sizeof(uint32_t)]);
    if (pred_codewords.find(codeword) == pred_codewords.end()) {
      BitmapSet(sel->mutable_bitmap(), offset+i);
    }
  }
  offset += nrows;
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
