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

class SecondaryIndex {
  public:
  	// input would be ColumnPredicate
  	// output would be SelectionVector
    Status Scan();
}

} // namespace kudu

#endif // KUDU_CFILE_SECONDARY_INDEX_H