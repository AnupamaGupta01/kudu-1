#ifndef PROJECT_COLUMN_EVAL_CONTEXT_H
#define PROJECT_COLUMN_EVAL_CONTEXT_H

#include "kudu/common/column_predicate.h"
#include "kudu/common/columnblock.h"

// A ColumnEvalContext provides a clean interface to the set of objects that get passed down to
// the decoders during predicate pushdown
namespace kudu {


class ColumnEvalContext {
public:
  ColumnEvalContext(const ColumnPredicate &pred,
                    ColumnBlock *block,
                    SelectionVector *sel,
                    bool &eval_complete) :
          pred_(pred), block_(block), sel_(sel), eval_complete_(eval_complete) {};

  const ColumnPredicate &pred() { return pred_; }

  SelectionVector *sel() { return sel_; }

  ColumnBlock *block() { return block_; }

  bool &eval_complete() { return eval_complete_; }

private:
  // Predicate being evaluated
  const ColumnPredicate &pred_;

  // Location where data will be copied to
  ColumnBlock *block_;

  // Selection vector reflecting the result of the predicate evaluation
  SelectionVector *sel_;

  // Determines whether further evaluation is needed
  bool &eval_complete_;
};

}
#endif //PROJECT_COLUMN_EVAL_CONTEXT_H

