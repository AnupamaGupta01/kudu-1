#ifndef KUDU_CFILE_BITMAP_INDEX_H
#define KUDU_CFILE_BITMAP_INDEX_H

#include "kudu/common/slice.h"
#include "kudu/gutil/macros.h"
#include "kudu/util/bitmap.h"
#include "kudu/util/rowblock.h"

class BitmapIndexBuilder : public SecondaryIndexBuilder {
  public:
  	explicit BitmapIndexBuilder();
  	void Clear();
  	void AddData();
  	void UpdateData();
    void DeleteData();
  	// Return multiple slices? Iterator to slices? Depends on how we want to serialize the data
  	const Slice slice() const {
  	  return Slice();
  	}
    size_t count_rows();
  private:
}

class BitmapIndex : public SecondaryIndex {
public:
  Status Scan();
private:
  std::vector<SelectionVector *> bitmaps_;
  std::unordered_map<StringPiece, uint32_t, GoodFastHash<StringPiece> > dictionary_;
} // namespace kudu

#endif // KUDU_CFILE_BITMAP_INDEX_H