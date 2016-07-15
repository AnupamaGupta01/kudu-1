#ifndef KUDU_CFILE_BITMAPFILE_H
#define KUDU_CFILE_BITMAPILE_H

#include <memory>
#include <string>
#include <vector>

#include "kudu/cfile/secondary_index_file.h"

namespace kudu {
namespace cfile {

class BitmapFileWriter : public SecondaryIndexWriter {
  public:
  	Status Start();
  	Status AddValues();
  	Status Finish();
  	Status FinishAndReleaseBlock(fs::ScopedWriteableBlockCloser* closer);
  	size_t written_size() const;
  private:
  	// The purpose of using a builder rather than the actual bitmap is to access slices into the bitmap
  	BitmapIndexBuilder index_builder_;
  	Status FinishCurrentSecondaryIndexBlock();

    // Block pointing to the dictionary
    BinaryPlainBockBuilder dict_block_;
   	gscoped_ptr<cfile::CFileWriter> writer_;
}

class BitmapFileReader : public SecondaryIndexReader {
  public:
    Status Open(gscoped_ptr<fs::ReadableBlock> block,
                const ReaderOptions& options,
                gscoped_ptr<SecondaryIndexReader> *reader);
    Status OpenNoInit(gscoped_ptr<fs::ReadableBlock> block,
                const ReaderOptions& options,
                gscoped_ptr<SecondaryIndexReader> *reader);
    Status Init();
    Status CheckValueExists(bool *maybe_present);
  private:
    BitmapFileReader(gscoped_ptr<CFileReader> reader, const ReaderOptions& options);
    Status ParseBlockHeader(const Slice &block,
                            SecondaryIndexHeaderPB *hdr,
                            Slice *index_data) const;
    Status InitOnce();
    size_t memory_footprint_excluding_reader() const;

    // Dictionary block decoder
    BinaryPlainBlockDecoder* dict_decoder_;
    gscoped_ptr<CFileReader> reader_;
}

} // namespace cfile
} // namespace kudu

#endif
