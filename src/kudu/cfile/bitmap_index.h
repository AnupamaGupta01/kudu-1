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
}

// This should be a template for DataType
template <DataType Type>
class BitmapIndex : public SecondaryIndex<DataType> {
public:
  void Equals(const void* value, SelectionVector *sel);
  void Range(const void* lower, const void* upper, SelectionVector *sel);

private:
  // // store the bitmap and the number of 1s
  // // this would be helpful to cheaply check the for existence
  // std::vector<std::tuple<SelectionVector *, uint32_t>> bitmaps_;

  std::map<Type, SelectionVector *> bitmaps_;
  // // store the value and the index in bitmaps_ of the proper SelectionVector
  // // stored in order so we can evaluate inequality predicates
  // std::map<DataType, uint32_t, GoodFastHash<DataType> > dictionary_;

} // namespace kudu

#endif // KUDU_CFILE_BITMAP_INDEX_H