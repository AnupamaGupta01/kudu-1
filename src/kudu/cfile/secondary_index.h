#ifndef KUDU_CFILE_SECONDARY_INDEX_H
#define KUDU_CFILE_SECONDARY_INDEX_H

#include "kudu/gutil/gscoped_ptr.h"
#include "kudu/util/slice.h"

namespace kudu {

class SecondaryIndexBuilder {
  public:
    void Clear();
    void AddData();
    void UpdateData();
    void DeleteData();
    const Slice slice() const;
    size_t count_rows();
}

template <DataType Type>
class SecondaryIndex {
  public:
  	// input would be ColumnPredicate
  	// output would be SelectionVector
    void Equals(const void* value, SelectionVector *sel);
    void Range(const void* lower, const void* upper, SelectionVector *sel);
    // void NotNull();
}

} // namespace kudu

#endif // KUDU_CFILE_SECONDARY_INDEX_H