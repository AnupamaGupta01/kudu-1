#include "kudu/cfile/bitmapfile.h"

BitmapFileWriter::BitmapFileWriter(gscoped_ptr<WritableBlock> block) {
  cfile::WriterOptions opts;
  
  // TODO what are these
  opts.write_posidx = false;
  opts.write_validx = true;

}

Status BitmapFileWriter::Start() {
  return writer_->Start();
}

Status BitmapFileWriter::Finish() {
  ScopedWritableBlockCloser closer;
  RETURN_NOT_OK(FinishAndReleaseBlock(&closer));
  return closer.CloseBlocks();
}

Status BitmapFileWriter::FinishAndReleaseBlock(ScopedWritableBlockCloser* closer) {
  if (index_builder_.count() > 0) {
  	// Finish writing to the current index block
  	RETURN_NOT_OK(FinishCurrentBitmapBlock());
  }
  return writer_->FinishAndReleaseBlock(closer);
}

size_t BitmapFileWriter::written_size() const {
  return writer_->written_size();
}

Status BitmapFileWriter::AddValues(const void *entries, size_t count) {
  // TODO @andrwng: Actually implement this
  index_builder_.AddData();
}

Status BitmapFileWriter::AddValues(size_t row_idx, Cell cell) {
  // TODO @andrwng: Actually implement this
  index_builder_.AddData(row_idx, cell);
}

Status BitmapFileWriter::AddValues(ColumnBlock cblock) {

}

Status BitmapFileWriter::FinishCurrentBitmapBlock() {
  // This is the function that actually writes the bitmap to disk
  // Encode the header
  // Append the bitmap slices
  // Append the whole thing to the file
  vector<Slice> slices;
  slices.push_back(Slice(hrd_str));
  slices.push_back(bitmap_builder_.)
}

///////////////////////////////////////////////////////
// Reader
///////////////////////////////////////////////////////

Status BitmapFileReader::Open(gscoped_ptr<fs::ReadableBlock> block,
                const ReaderOptions& options,
                gscoped_ptr<SecondaryIndexReader> *reader) {
  gscoped_ptr<BitmapFileReader> bm_reader;
  RETURN_NOT_OK(OpenNoInit(std::move(block), options, &bf_reader));
  RETURN_NOT_OK(bm_reader->Init());

  *reader = std::move(bf_reader);
  return Status::OK();
}

Status BitmapFileReader::OpenNoInit(gscoped_ptr<fs::ReadableBlock> block,
                const ReaderOptions& options,
                gscoped_ptr<SecondaryIndexReader> *reader) {
  gscoped_ptr<CFileReader> cf_reader;
  RETURN_NOT_OK(CFileReader::OpenNoInit(std::move(block), options, &cf_reader));
  gscoped_ptr<BitmapFileReader> bm_reader(new BitmapFileReader(
      std::move(cf_reader), options));
  if (!FLAGS_cfile_lazy_open) {
    RETURN_NOT_OK(bm_reader->Init());
  }

  *reader = std::move(bf_reader);
  return Status::OK();
}

BitmapFileReader::BitmapFileReader(gscoped_ptr<CFileReader> reader,
                                   const ReaderOptions& options)
  : reader_(std::move(reader)),
    mem_consumption_(options.parent_mem_tracker,
                     memory_footprint_excluding_reader()) {
}

Status BitmapFileReader::Init() {
  RETURN_NOT_OK(reader_->Init());
  // look at how cfile_writer does this
  BlockPointer validx_root = reader_->validx_root();

  mem_consumption_.Reset(memory_footprint_excluding_reader());
  return Status::OK();
}

Status BitmapFileReader::CheckValueExists(bool *maybe_present) {

}

// or something
Status BitmapFileReader::Evaluate(ColumnPredicate p, SelectionVector *sel) {
  // Seek to the location of the BitmapIndex
  BlockHandle dblk_data;
  BlockPointer bblk_ptr;
  {
    std::unique_lock<simple_spinlock> lock;
    while (true) {
      std::unique_lock<simple_spinlock> l(iter_locks_[cpu], std::try_to_lock);
      if (l.owns_lock()) {
        lock.swap(l);
        break;
      }
      cpu = (cpu + 1) % index_iters_.size();
    }

    cfile::IndexTreeIterator *index_iter = index_iters_[cpu].get();

    Status s = index_iter->SeekAtOrBefore(probe.key());
    if (PREDICT_FALSE(s.IsNotFound())) {
      // Seek to before the first entry in the file.
      *maybe_present = false;
      return Status::OK();
    }
    RETURN_NOT_OK(s);

    // Successfully found the pointer to the bloom block. Read it.
    bblk_ptr = index_iter->GetCurrentBlockPointer();
  }
  RETURN_NOT_OK(reader_->ReadBlock(bblk_ptr, CFileReader::CACHE_BLOCK, &dblk_data));
  BitmapBlockHeaderPB hdr;
  Slice bitmap_data;
  RETURN_NOT_OK(ParseBlockHeader(dblk_data.data(), &hdr, &bitmap_data));

  BitmapIndex bm_index(bitmap_data);


  return Status::OK();
}