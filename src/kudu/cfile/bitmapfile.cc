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

Status BitmapFileWriter::AddValues(/* celltype */) {

}

Status BitmapFileWriter::FinishCurrentBitmapBlock() {
  // Encode the header
  // Append the bitmap slices
  // Append the whole thing to the file
}

///////////////////////////////////////////////////////
// Reader
///////////////////////////////////////////////////////

Status BitmapFileReader::Open(gscoped_ptr<fs::ReadableBlock> block,
                const ReaderOptions& options,
                gscoped_ptr<SecondaryIndexReader> *reader) {

}

Status BitmapFileReader::OpenNoInit(gscoped_ptr<fs::ReadableBlock> block,
                const ReaderOptions& options,
                gscoped_ptr<SecondaryIndexReader> *reader) {

}

Status BitmapFileReader::Init() {
  RETURN_NOT_OK(reader_->Init());

  BlockPointer validx_root = reader_->validx_root();
  
}

Status BitmapFileReader::CheckValueExists(bool *maybe_present) {

}

// or something
Status BitmapFileReader::Evaluate(ColumnPredicate p) {
  // Seek to the location of the BitmapIndex
  BlockHandle dblk_data;
  RETURN_NOT_OK(reader_->ReadBlock(bblk_ptr, CFileReader::CACHE_BLOCK, &dblk_data));
  Slice bitmap_data;
  RETURN_NOT_OK(ParseBlockHeader(dblk_data.data(), &hdr, &bitmap_data));

  BitmapIndex bidx(bitmap_data);
  return Status::OK();
}