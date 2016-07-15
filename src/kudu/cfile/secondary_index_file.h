#include <memory>
#include <string>
#include <vector>

#include "kudu/cfile/cfile_reader.h"
#include "kudu/cfile/cfile_writer.h"
#include "kudu/cfile/secondary_index.h"
#include "kudu/gutil/macros.h"
#include "kudu/util/block_encodings.h"
#include "kudu/util/faststring.h"
#include "kudu/util/mem_tracker.h"
#include "kudu/util/once.h"
#include "kudu/util/status.h"

// If confused, see bloomfile.h
// These should all be virtual
class SecondaryIndexWriter {
  public:
    SecondaryIndexWriter();
    virtual Status Start();
    virtual Status AddValues();
    virtual Status Finish();
    virtual Status FinishAndReleaseBlock(fs::ScopedWriteableBlockCloser* closer);
    virtual size_t written_size() const;
   private:
   	Status FinishCurrentSecondaryIndexBlock();
   	gscoped_ptr<cfile::CFileWriter> writer_;
   	SecondaryIndexBuilder index_builder_;
}

class SecondaryIndexReader {
  public:
  	virtual Status Open(gscoped_ptr<fs::ReadableBlock> block,
                const ReaderOptions& options,
                gscoped_ptr<SecondaryIndexReader> *reader);
  	virtual Status OpenNoInit(gscoped_ptr<fs::ReadableBlock> block,
                const ReaderOptions& options,
                gscoped_ptr<SecondaryIndexReader> *reader);
  	virtual Status Init();
  	virtual Status CheckValueExists(bool *maybe_present);
  private:
  	SecondaryIndexReader(gscoped_ptr<CFileReader> reader, const ReaderOptions& options);
  	// Returns the parsed header inside *hdr, and returns
  	// a Slice to the secondary index inside *index_data
  	Status ParseBlockHeader(const Slice &block,
  							SecondaryIndexHeaderPB *hdr,
  							Slice *index_data) const;
  	Status InitOnce();
  	size_t memory_footprint_excluding_reader() const;
  	gscoped_ptr<CFileReader> reader_;
}