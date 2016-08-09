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

// TODO: change name to SortedVocabBlock*
namespace kudu {
namespace cfile {

SortedPlainBlockBuilder::SortedPlainBlockBuilder(const WriterOptions *options)
  : vocab_builder_(new BinaryPlainBlockBuilder(options)),
    sort_builder_(new BShufBlockBuilder<UINT32>(options)),
    dictionary_strings_arena_(1024, 32*1024*1024) {
  Reset();
}

void SortedPlainBlockBuilder::Reset() {
  vocab_builder_->Reset();
  sort_builder_->Reset();
}

bool SortedPlainBlockBuilder::IsBlockFull(size_t limit) const {
  return vocab_builder_->IsBlockFull(limit);
}

Slice SortedPlainBlockBuilder::Finish(rowid_t ordinal_pos) {
  finished_ = true;

  // vocab builder has its own memory location with its own buffer
  Slice vocab_slice = vocab_builder_->Finish(dictionary_.size());

  // Add the size of the data_slice to the buffer
  // This will provide a way to determine the location of the sorted values as well
  size_t vocab_slice_size = vocab_slice.size();
  //  InlineEncodeFixed32(&buffer_[0], vocab_slice_size);

  buffer_.append(static_cast<void *>(&vocab_slice_size), 4);
  buffer_.append(vocab_slice.data(), vocab_slice.size());

  // Sort through the words and sort them, recording the codewords as well
  // Iterate through the sorted codewords and Add each of them to the sort_builder
  std::vector<StringPiece> words;
  for (auto& vocab_entry : dictionary_) {
    words.push_back(vocab_entry.first);
  }
  std::sort(words.begin(), words.end());
  for (auto& word : words) {
    sort_builder_->Add(reinterpret_cast<const uint8_t*>(&(dictionary_[word])), 1);
  }

  // TODO: maybe add the rank_of_codeword as another decoder
  // sort builder has its own memory location with its own buffer
  Slice sort_slice = sort_builder_->Finish(words.size());
  buffer_.append(sort_slice.data(), sort_slice.size());

  return Slice(buffer_);
}

uint32_t SortedPlainBlockBuilder::CodewordOf(StringPiece word) const {
  return dictionary_.at(word);
}

int SortedPlainBlockBuilder::Add(const uint8_t *vals, size_t count) {
  // Return 0 if the codeword cannot be added to the dictionary
  // Adds the value to the dictionary, does nothing if already in the dict
  // For now, ignore count and only add 1
  const Slice* src = reinterpret_cast<const Slice*>(vals);
  const char* c_str = reinterpret_cast<const char*>(src->data());
  StringPiece current_item(c_str, src->size());
  uint32_t codeword;
  if (!FindCopy(dictionary_, current_item, &codeword)) {
    // The dictionary block is full
    if (vocab_builder_->Add(vals, 1) == 0) {
      return 0;
    }
    // Adds the slice to the dictionary arena
    const uint8_t* s_ptr = dictionary_strings_arena_.AddSlice(*src);
    if (s_ptr == nullptr) {
      // Arena does not have enough space for string content
      // Ideally, it should not happen.
      LOG(ERROR) << "Arena of Dictionary Encoder does not have enough memory for strings";
      return 0;
    }
    const char* s_content = reinterpret_cast<const char*>(s_ptr);
    codeword = vocab_builder_->Count() - 1;
    InsertOrDie(&dictionary_, StringPiece(s_content, src->size()), codeword);
  }
  return 1;
}

size_t SortedPlainBlockBuilder::Count() const {
  return vocab_builder_->Count();
}

Status SortedPlainBlockBuilder::GetFirstKey(void *key_void) const {
  RETURN_NOT_OK(vocab_builder_->GetFirstKey(key_void));
  return Status::OK();
}

////////////////////////////////////////////////////////////
// Decoding
////////////////////////////////////////////////////////////

SortedPlainBlockDecoder::SortedPlainBlockDecoder(Slice slice)
        : data_(std::move(slice)),
          parsed_(false) {
}

Status SortedPlainBlockDecoder::ParseHeader() {
  CHECK(!parsed_);
  if (data_.size() < kMinHeaderSize) {
    return Status::Corruption(
            strings::Substitute("not enough bytes for header: dictionary block header "
                                        "size ($0) less than minimum possible header length ($1)",
                                data_.size(), kMinHeaderSize));
  }
  size_t vocab_slice_size = DecodeFixed32(&data_[0]);
  Slice vocab_content(data_.data() + 4, vocab_slice_size);
  vocab_decoder_.reset(new BinaryPlainBlockDecoder(vocab_content));
  RETURN_NOT_OK(vocab_decoder_->ParseHeader());

  Slice sort_content(data_.data() + 4 + vocab_slice_size, data_.size() - 4 - vocab_slice_size);
  sort_decoder_.reset(new BShufBlockDecoder<UINT32>(sort_content));
  RETURN_NOT_OK(sort_decoder_->ParseHeader());

  // TODO: Move this into another tacked-on block to the SortedPlainBlock
  // Note that every BShufBlock is not O(1) access

  rank_of_codeword_.resize(sort_decoder_->Count());
  size_t one = 1;
  for (int i = 0; i < sort_decoder_->Count(); i++) {
    uint32_t ith;
    RETURN_NOT_OK(sort_decoder_->CopyNextValuesToArray(&one, reinterpret_cast<uint8_t*>(&ith)));
    rank_of_codeword_[ith] = i;
  }
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

// Return the rank of a codeword
// Used to determine whether a codeword is within the bounds of the codeword ranges
uint32_t SortedPlainBlockDecoder::RankOfCodeword(uint32_t codeword) const {
  if (codeword == rank_of_codeword_.size()) {
    return codeword;
  }
  return rank_of_codeword_[codeword];
}

// Return the <rank>th codeword of in the dictionary
// Used to search for the codewords corresponding to the predicate bounds
uint32_t SortedPlainBlockDecoder::CodewordAtRank(uint32_t rank) {
  // TODO: there must be a cleaner way to do this. Perhaps add a pointer interface into bshuf_block
  sort_decoder_->SeekToPositionInBlock(rank);
  uint32_t ret;
  size_t one = 1;
  sort_decoder_->CopyNextValuesToArray(&one, reinterpret_cast<uint8_t *>(&ret));
  return ret;
}

Status SortedPlainBlockDecoder::SeekAtOrAfterValue(const void* value_void, bool* exact) {
  return vocab_decoder_->SeekAtOrAfterValue(value_void, exact);
}

Status SortedPlainBlockDecoder::SeekAtOrAfterWord(const void *value_void, bool *exact, uint32_t& codeword) {
  DCHECK(value_void != nullptr);

  const Slice &target = *reinterpret_cast<const Slice *>(value_void);

  // Binary search in restart array to find the first restart point
  // with a key >= target
  codeword = 0;
  int32_t left = 0;
  int32_t right = sort_decoder_->Count();
  while (left != right) {
    // referring to the middle of the sorted codewords
    uint32_t mid = (left + right) / 2;

    // vocab_decoder_->codeword_with_rank()
    uint32_t mid_codeword = CodewordAtRank(mid);

    // referring to the string corresponding to the middle

    Slice mid_key(vocab_decoder_->string_at_index(mid_codeword));
    int c = mid_key.compare(target);
    if (c < 0) {
      left = mid + 1;
    } else if (c > 0) {
      right = mid;
    } else {
      codeword = mid_codeword;
      *exact = true;
      return Status::OK();
    }
  }
  *exact = false;
  if (left == sort_decoder_->Count()) {
    codeword = sort_decoder_->Count();
    return Status::OK();
  }
  codeword = CodewordAtRank(left);
  return Status::OK();
}

Status SortedPlainBlockDecoder::CopyNextValues(size_t *n, ColumnDataView *dst) {
  return vocab_decoder_->CopyNextValues(n, dst);
}

} // namespace cfile
} // namespace kudu
