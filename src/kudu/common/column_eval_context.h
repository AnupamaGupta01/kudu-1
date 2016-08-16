#ifndef PROJECT_COLUMN_EVAL_CONTEXT_H
#define PROJECT_COLUMN_EVAL_CONTEXT_H

#include "kudu/common/column_predicate.h"
#include "kudu/common/columnblock.h"

// A ColumnEvalContext provides a clean interface to the set of objects that get passed down to
// the decoders during predicate pushdown
namespace kudu {

class ColumnEvalContext {
public:
  ColumnEvalContext(const size_t col_idx,
                    const ColumnPredicate &pred,
                    ColumnBlock *block,
                    SelectionVector *sel,
                    bool &eval_complete) :
          col_idx_(col_idx), pred_(pred), block_(block), sel_(sel), eval_complete_(eval_complete) {};

  // Column index in the parent CFileSet
  const size_t col_idx() { return col_idx_; }

  // Predicate being evaluated
  const ColumnPredicate &pred() { return pred_; }

  // Location where data will be copied to
  ColumnBlock *block() { return block_; }

  // Selection vector reflecting the result of the predicate evaluation
  SelectionVector *sel() { return sel_; }

  // Determines whether further evaluation is needed.
  // Set to false only if the encoding type does not support decoder evaluation
  bool &eval_complete() { return eval_complete_; }

private:
  const size_t col_idx_;

  const ColumnPredicate &pred_;

  // ColumnBlock *block_;
  ColumnBlock *block_;

  //  SelectionVector *sel_;
  SelectionVector *sel_;

  bool &eval_complete_;
};

}
#endif //PROJECT_COLUMN_EVAL_CONTEXT_H

