#ifndef KUDU_TABLET_ROWSET_BITMAP_H
#define KUDU_TABLET_ROWSET_BITMAP_H

#include <unordered_map>
#include <vector>
#include <utility>

#include "kudu/gutil/gscoped_ptr.h"
#include "kudu/gutil/map-util.h"
#include "kudu/util/status.h"
#include "kudu/util/bitmap.h"
#include "kudu/tablet/rowset.h"
namespace kudu {

template<class Traits>

namespace tablet {

// struct RowSetIntervalTraits;

struct RowSetWithBounds;

// @andrwng
// Class that manages the non-key columns of a rowset for a given Tablet.
// This allows for additional culling when performing a scan for a non-key
// column only selecting rowsets that are relevant to the scan.
//
// The scan must specify the bounds that are being scanned that are not
// the non-key columns.
//
// This should act very similarly to rowset_tree, and should only focus on
// a single column, so for any given query/scan on a non-key column, we should
// consult the corresponding RowSetBoundsList
class RowSetBitmap {
  public:
    RowSetBitmap();
    Status Reset(const RowSetVector &rowsets);
    ~RowSetBitmap();

    // 1. decode from the string
    // 2. encode using one-hot encoding (see tablet.cc to get other columns; they 
    //    use keyprobe, which is only the primary key. Ask how to get the non-keys)
    // 3. one-hot encode
    void AddRow();

    // Return all RowSets whose column col_idx may contain the given encoded key.
    void FindRowSetsWithValueEqual(const Slice &encoded_key,
                                   int col_idx,
                                   std::vector<RowSet *> *rowsets) const;

    const RowSetVector &all_rowsets() const { return all_rowsets_; }

  private:
    // vector of columns corresponding to one bitmap per column
    std::vector<uint8_t *> column_bitmaps_;

    // Need some way of one-hot encoding the column values, and keeping it up-to-date.
    // May not be the same across all tablets, as long as it is correct within a rowset
    //    e.g. if two tablets have two unique values for a given column (totalling four
    //         values), they should both have two-bit represented columns
    // TODO: look at how rowset_tree does this, if at all

    // Flattened view of the rowset, should correspond to bitwise or over rows
    std::vector<uint8_t *> flatted_columns_;
}


} // namespace tablet
} // namespace kudu
#endif // KUDU_TABLET_ROWSET_BITMAP_H