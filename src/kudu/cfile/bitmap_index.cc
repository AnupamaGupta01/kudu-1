#include "kudu/cfile/bitmap_index.h"
// A bitmap index indexes a single column, storing n bitmaps for a column
// whose values take n unique values

void BitmapIndexBuider::Clear() {

}

void BitmapIndexBuider::AddData() {

}

void BitmapIndexBuider::UpdateData() {

}

void BitmapIndexBuider::DeleteData() {

}

size_t BitmapIndexBuilder::count_rows() {

}

BitmapIndex::Equals(const void* value, SelectionVector *sel) {
  // Get the proper column given the column_name and copy the SelectionVector

}

BitmapIndex::Range(const void* lower,
                   const void* upper,
                   SelectionVector *sel) {
  
}
