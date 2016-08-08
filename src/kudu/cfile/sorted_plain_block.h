#ifndef KUDU_CFILE_SORTED_PLAIN_BLOCK_H
#define KUDU_CFILE_SORTED_PLAIN_BLOCK_H

#include <vector>

#include "kudu/cfile/block_encodings.h"
#include "kudu/util/faststring.h"
#include "kudu/util/slice.h"
#include "bshuf_block.h"
#include "binary_plain_block.h"

namespace kudu {
namespace cfile {

struct WriterOptions;

class SortedPlainBlockBuilder : public BlockBuilder {
public:
  explicit SortedPlainBlockBuilder(const WriterOptions *options);

  bool IsBlockFull(size_t limit) const OVERRIDE;

  int Add(const uint8_t *vals, size_t count) OVERRIDE;

  // Return a Slice which represents the encoded data.
  //
  // This Slice points to internal data of this class
  // and becomes invalid after the builder is destroyed
  // or after Finish() is called again.
  Slice Finish(rowid_t ordinal_pos) OVERRIDE;

  void Reset() OVERRIDE;

  size_t Count() const OVERRIDE;

  // Return the first added key.
  // key should be a Slice *
  Status GetFirstKey(void *key) const OVERRIDE;

  // Length of a header.
  static const size_t kMaxHeaderSize = sizeof(uint32_t) * 3;

private:
  faststring buffer_;

  size_t end_of_data_offset_;
  size_t size_estimate_;

  std::map<StringPiece, uint32_t> dictionary_;
  gscoped_ptr<BinaryPlainBlockBuilder> dict_builder_;
  gscoped_ptr<BShufBlockBuilder> sort_builder_;

  bool finished_;

  const WriterOptions *options_;

};

class SortedPlainBlockDecoder : public BlockDecoder {
public:
  explicit SortedPlainBlockDecoder(Slice slice);

  virtual Status ParseHeader() OVERRIDE;
  virtual void SeekToPositionInBlock(uint pos) OVERRIDE;
  virtual Status SeekAtOrAfterValue(const void *value,
                                    bool *exact_match) OVERRIDE;
  Status CopyNextValues(size_t *n, ColumnDataView *dst) OVERRIDE;

  virtual bool HasNext() const OVERRIDE {
    DCHECK(parsed_);
    return cur_idx_ < num_elems_;
  }

  virtual size_t Count() const OVERRIDE {
    DCHECK(parsed_);
    return num_elems_;
  }

  virtual size_t GetCurrentIndex() const OVERRIDE {
    DCHECK(parsed_);
    return cur_idx_;
  }

  virtual rowid_t GetFirstRowId() const OVERRIDE {
    return ordinal_pos_base_;
  }

  uint32_t codeword_at_rank(size_t idx) const;

  Slice string_at_index(size_t idx) const {
    const uint32_t offset = offsets_[idx];
    uint32_t len = offsets_[idx + 1] - offset;
    return Slice(&data_[offset], len);
  }

  // Minimum length of a header.
  static const size_t kMinHeaderSize = sizeof(uint32_t) * 3;

private:
  Slice data_;
  bool parsed_;

  // The parsed offsets.
  // This array also contains one extra offset at the end, pointing
  // _after_ the last entry. This makes the code much simpler.
  std::vector<uint32_t> offsets_;

  uint32_t num_elems_;
  rowid_t ordinal_pos_base_;


  gscoped_ptr<BinaryPlainBlockBuilder> dict_decoder_;
  gscoped_ptr<BShufBlockDecoder> sort_decoder_;

  // Index of the currently seeked element in the block.
  uint32_t cur_idx_;
};

} // namespace cfile
} // namespace kudu

#endif // KUDU_CFILE_SORTED_PREFIX_BLOCK_H
