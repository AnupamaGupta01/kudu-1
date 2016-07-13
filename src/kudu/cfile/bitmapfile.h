#ifndef KUDU_CFILE_BITMAPFILE_H
#define KUDU_CFILE_BITMAPILE_H

#include <memory>
#include <string>
#include <vector>

#include "kudu/cfile/cfile_reader.h"
#include "kudu/cfile/cfile_writer.h"
#include "kudu/gutil/macros.h"
#include "kudu/util/bitmap.h"
#include "kudu/util/faststring.h"
#include "kudu/util/mem_tracker.h"
#include "kudu/util/once.h"
#include "kudu/util/status.h"

namespace kudu {
namespace cfile {

class BitmapFileWriter : public BinaryDictBlockBuilder {
 public:
  BitmapFileWriter(gscoped_ptr<fs::WritableBlock> block);

  Status Start();
  // Status AppendKeys(const Slice *keys, size_t n_keys);

  // Close the bitmaps's CFile, closing the underlying writable block.
  Status Finish();

  // Close the bitmap's CFile, releasing the underlying block to 'closer'.
  Status FinishAndReleaseBlock(fs::ScopedWritableBlockCloser* closer);

  // Estimate the amount of data already written to this file.
  size_t written_size() const;

 private:
  DISALLOW_COPY_AND_ASSIGN(BloomFileWriter);

  Status FinishCurrentBitmapBlock();

  gscoped_ptr<cfile::CFileWriter> writer_;

  BitmapBuilder bitmap_builder_;

  // first key inserted in the current block.
  faststring first_key_;
};

// Reader for a bitmap
class BitmapFileReader {
 public:

  // Fully open a bloom file using a previously opened block.
  //
  // After this call, the bloom reader is safe for use.
  static Status Open(gscoped_ptr<fs::ReadableBlock> block,
                     const ReaderOptions& options,
                     gscoped_ptr<BitmapFileReader> *reader);

  // Lazily opens a bloom file using a previously opened block. A lazy open
  // does not incur additional I/O, nor does it validate the contents of
  // the bloom file.
  //
  // Init() must be called before using CheckKeyPresent().
  static Status OpenNoInit(gscoped_ptr<fs::ReadableBlock> block,
                           const ReaderOptions& options,
                           gscoped_ptr<BloomFileReader> *reader);

  // Fully opens a previously lazily opened bitmap file, parsing and
  // validating its contents.
  //
  // May be called multiple times; subsequent calls will no-op.
  Status Init();

  // Check if the given key may be present in the file.
  //
  // Sets *maybe_present to false if the key is definitely not
  // present, otherwise sets it to true to indicate maybe present.
  Status CheckKeyPresent(const BloomKeyProbe &probe,
                         bool *maybe_present);

  // @andrwng
  // Return the row indices within the bitmap where the column value matches the predicate
  // Decode the predicate, determine the column index for that predicate, and scan
  Status DeterminePredicateLocations(Predicate pred, std::vector<uint8_t *> locs);


 private:
  DISALLOW_COPY_AND_ASSIGN(BloomFileReader);

  BitmapFileReader(gscoped_ptr<CFileReader> reader, const ReaderOptions& options);

  // Parse the header present in the given block.
  //
  // Returns the parsed header inside *hdr, and returns
  // a Slice to the true bloom filter data inside
  // *bitmap_data.
  Status ParseBlockHeader(const Slice &block,
                          BitmapBlockHeaderPB *hdr,
                          Slice *bitmap_data) const;

  // Callback used in 'init_once_' to initialize this bloom file.
  Status InitOnce();

  // Returns the memory usage of this object including the object itself but
  // excluding the CFileReader, which is tracked independently.
  size_t memory_footprint_excluding_reader() const;

  gscoped_ptr<CFileReader> reader_;

  // TODO: temporary workaround for the fact that
  // the index tree iterator is a member of the Reader object.
  // We need a big per-thread object which gets passed around so as
  // to avoid this... Instead we'll use a per-CPU iterator as a
  // lame hack.
  std::vector<std::unique_ptr<cfile::IndexTreeIterator>> index_iters_;
  gscoped_ptr<padded_spinlock[]> iter_locks_;

  KuduOnceDynamic init_once_;

  ScopedTrackedConsumption mem_consumption_;
};

} // namespace cfile
} // namespace kudu

#endif
