#include "kudu/cfile/sorted_plain_block.h"

#include <glog/logging.h>
#include <algorithm>

#include "kudu/cfile/cfile_util.h"
#include "kudu/cfile/cfile_writer.h"
#include "kudu/common/columnblock.h"
#include "kudu/gutil/stringprintf.h"
#include "kudu/util/coding.h"
#include "kudu/util/coding-inl.h"
#include "kudu/util/group_varint-inl.h"
#include "kudu/util/hexdump.h"
#include "kudu/util/memory/arena.h"

namespace kudu {
namespace cfile {

SortedPlainBlockBuilder::SortedPlainBlockBuilder(const WriterOptions *options)
  : dict_builder_(options),
    sort_builder_(options) {
  Reset();
}

void SortedPlainBlockBuilder::Reset() {
  dict_builder_.Reset();
  sort_builder_.Reset();
}

bool SortedPlainBlockBuilder::IsBlockFull(size_t limit) const {
  return size_estimate_ > limit;
}

Slice SortedPlainBlockBuilder::Finish(rowid_t ordinal_pos) {
  Slice sorted = sort_builder_->Finish(ordinal_pos);
  dict_builder_->Finish(ordinal_pos);

  //  buffer_.append(static_cast<void *>(&mode_), 4);
  // TODO: if we could modify the the Finish() API a little bit, we can
  // avoid an extra memory copy (buffer_.append(..))
  Slice dict_slice = dict_builder_->Finish(ordinal_pos);

  // Add the size of the data_slice to the buffer
  // This will provide a way to determine the location of the sorted values as well
  uint32_t dict_slice_size = dict_slice.size();
  buffer_.append(static_cast<void *>(&dict_slice_size), 4);
  //  InlineEncodeFixed32(&buffer_[4], data_slice.size());
  buffer_.append(dict_slice.data(), dict_slice.size());

  // Iterate through the sorted codewords and Add each of them to the sort_builder
  uint32_t i = 0;
  for (auto iter =  dictionary_.begin(); iter != dictionary_.end(); iter++) {
    if (sort_builder_->Add(reinterpret_cast<const uint8_t*>(&(iter->second)), 1) == 0) {
      break;
    }
    i++;
  }
  Slice sort_slice = sort_builder_->Finish(i);
  buffer_.append(sort_slice.data(), sort_slice.size());

  return Slice(buffer_);
}

int SortedPlainBlockBuilder::Add(const uint8_t *vals, size_t count) {
  int i = dict_builder_->Add(vals, count);


  return i;
}

int SortedPlainBlockBuilder::AddCodeWords(const uint8_t* vals, size_t count) {
  DCHECK(!finished_);
  DCHECK_GT(count, 0);
  size_t i;

  for (i = 0; i < count; i++) {
    const Slice* src = reinterpret_cast<const Slice*>(vals);
    const char* c_str = reinterpret_cast<const char*>(src->data());
    StringPiece current_item(c_str, src->size());
    uint32_t codeword;

    if (!FindCopy(dictionary_, current_item, &codeword)) {
      // The dictionary block is full
      if (dict_builder_.Add(vals, 1) == 0) {
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
      codeword = dict_builder_.Count() - 1;
      InsertOrDie(&dictionary_, StringPiece(s_content, src->size()), codeword);
    }
    // The data block is full
    if (data_builder_->Add(reinterpret_cast<const uint8_t*>(&codeword), 1) == 0) {
      break;
    }
    // Keep the codewords sorted in memory
    vals += sizeof(Slice);
  }
  return i;
}

size_t SortedPlainBlockBuilder::Count() const {
  return dict_builder_.Count();
}

Status SortedPlainBlockBuilder::GetFirstKey(void *key_void) const {
  RETURN_NOT_OK(dict_builder_.GetFirstKey(key_void));
  return Status::OK();
}

////////////////////////////////////////////////////////////
// Decoding
////////////////////////////////////////////////////////////

SortedPlainBlockDecoder::SortedPlainBlockDecoder(Slice slice)
        : data_(std::move(slice)),
          parsed_(false),
          num_elems_(0),
          ordinal_pos_base_(0),
          cur_idx_(0) {
}

Status SortedPlainBlockDecoder::ParseHeader() {
  CHECK(!parsed_);

  if (data_.size() < kMinHeaderSize) {
    return Status::Corruption(
            strings::Substitute("not enough bytes for header: string block header "
                                        "size ($0) less than minimum possible header length ($1)",
                                data_.size(), kMinHeaderSize));
  }

  // Decode header.
  ordinal_pos_base_  = DecodeFixed32(&data_[0]);
  num_elems_         = DecodeFixed32(&data_[4]);
  size_t offsets_pos = DecodeFixed32(&data_[8]);

  // Sanity check.
  if (offsets_pos > data_.size()) {
    return Status::Corruption(
            StringPrintf("offsets_pos %ld > block size %ld in plain string block",
                         offsets_pos, data_.size()));
  }

  // Decode the string offsets themselves
  const uint8_t *p = data_.data() + offsets_pos;
  const uint8_t *limit = data_.data() + data_.size();

  offsets_.clear();
  offsets_.reserve(num_elems_);

  size_t rem = num_elems_;
  while (rem >= 4) {
    uint32_t ints[4];
    if (p + 16 < limit) {
      p = coding::DecodeGroupVarInt32_SSE(p, &ints[0], &ints[1], &ints[2], &ints[3]);
    } else {
      p = coding::DecodeGroupVarInt32_SlowButSafe(p, &ints[0], &ints[1], &ints[2], &ints[3]);
    }
    if (p > limit) {
      LOG(WARNING) << "bad block: " << HexDump(data_);
      return Status::Corruption(
              StringPrintf("unable to decode offsets in block"));
    }

    offsets_.push_back(ints[0]);
    offsets_.push_back(ints[1]);
    offsets_.push_back(ints[2]);
    offsets_.push_back(ints[3]);
    rem -= 4;
  }

  if (rem > 0) {
    uint32_t ints[4];
    p = coding::DecodeGroupVarInt32_SlowButSafe(p, &ints[0], &ints[1], &ints[2], &ints[3]);
    if (p > limit) {
      LOG(WARNING) << "bad block: " << HexDump(data_);
      return Status::Corruption(
              StringPrintf("unable to decode offsets in block"));
    }

    for (int i = 0; i < rem; i++) {
      offsets_.push_back(ints[i]);
    }
  }

  // Add one extra entry pointing after the last item to make the indexing easier.
  offsets_.push_back(offsets_pos);

  parsed_ = true;

  return Status::OK();
}

void SortedPlainBlockDecoder::SeekToPositionInBlock(uint pos) {
  if (PREDICT_FALSE(num_elems_ == 0)) {
    DCHECK_EQ(0, pos);
    return;
  }

  DCHECK_LE(pos, num_elems_);
  cur_idx_ = pos;
}

Status SortedPlainBlockDecoder::SeekAtOrAfterValue(const void *value_void, bool *exact) {
  DCHECK(value_void != nullptr);

  const Slice &target = *reinterpret_cast<const Slice *>(value_void);

  // Binary search in restart array to find the first restart point
  // with a key >= target
  int32_t left = 0;
  int32_t right = num_elems_;
  while (left != right) {
    uint32_t mid = (left + right) / 2;
    Slice mid_key(string_at_index(mid));
    int c = mid_key.compare(target);
    if (c < 0) {
      left = mid + 1;
    } else if (c > 0) {
      right = mid;
    } else {
      cur_idx_ = mid;
      *exact = true;
      return Status::OK();
    }
  }
  *exact = false;
  cur_idx_ = left;
  if (cur_idx_ == num_elems_) {
    return Status::NotFound("after last key in block");
  }

  return Status::OK();
}

Status SortedPlainBlockDecoder::CopyNextValues(size_t *n, ColumnDataView *dst) {
  DCHECK(parsed_);
  CHECK_EQ(dst->type_info()->physical_type(), BINARY);
  DCHECK_LE(*n, dst->nrows());
  DCHECK_EQ(dst->stride(), sizeof(Slice));

  Arena *out_arena = dst->arena();
  if (PREDICT_FALSE(*n == 0 || cur_idx_ >= num_elems_)) {
    *n = 0;
    return Status::OK();
  }

  size_t max_fetch = std::min(*n, static_cast<size_t>(num_elems_ - cur_idx_));

  Slice *out = reinterpret_cast<Slice *>(dst->data());
  size_t i;
  for (i = 0; i < max_fetch; i++) {
    Slice elem(string_at_index(cur_idx_));

    // TODO: in a lot of cases, we might be able to get away with the decoder
    // owning it and not truly copying. But, we should extend the CopyNextValues
    // API so that the caller can specify if they truly _need_ copies or not.
    CHECK(out_arena->RelocateSlice(elem, out));
    out++;
    cur_idx_++;
  }

  *n = i;
  return Status::OK();
}

} // namespace cfile
} // namespace kudu
