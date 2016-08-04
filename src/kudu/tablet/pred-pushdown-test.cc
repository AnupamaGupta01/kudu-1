// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless r ired by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include <glog/logging.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <vector>

#include "kudu/common/schema.h"

#include "kudu/cfile/cfile-test-base.h"
#include "kudu/cfile/binary_dict_block.h"
#include "kudu/common/column_predicate.h"
#include "kudu/common/schema.h"
#include "kudu/util/auto_release_pool.h"
#include "kudu/util/memory/arena.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"

namespace kudu {
namespace cfile {


enum Setup {
  ALL_IN_MEMORY,
  SPLIT_MEMORY_DISK,
  ALL_ON_DISK
};
// This benchmark generates multiple cfiles and puts [nrows_] data into them with the specified cardinality
  // TODO include the string length
class CardinalityDataGenerator : public DataGenerator<STRING, false> {
  public: 
    typedef typename DataTypeTraits<STRING>::cpp_type cpp_type;
    explicit CardinalityDataGenerator(const char* format, int cardinality)
    : format_(format), cardinality_(cardinality) {
      // total_entries_ = 0;
    }

    // Store the string in private member
    Slice BuildTestValue(size_t block_index, size_t value) OVERRIDE {
      data_buffers_[block_index] = StringPrintf(format_, value);
      return Slice(data_buffers_[block_index]);
    }
    
    void Resize(size_t num_entries) OVERRIDE {
      data_buffers_.resize(num_entries);
      values_.reset(new cpp_type[num_entries]);
    }

    void Build(size_t num_entries) {
      Build(0, num_entries);  // first arg is ignored
      // total_entries_ += num_entries;
    }
    
    void Build(size_t offset, size_t num_entries) {
      Resize(num_entries);

      for (size_t i = 0; i < num_entries; ++i) {
        values_[i] = BuildTestValue(i % cardinality_, i % cardinality_);
      }
    }
    const cpp_type *values() const {return values_.get(); }
  private:
    gscoped_array<cpp_type> values_;
    std::vector<std::string> data_buffers_;
    const char* format_;
    int cardinality_;
};

// class PredicatePushdownBenchmark
class PredicatePushdownTest : public CFileTestBase,
                           public ::testing::WithParamInterface<Setup> {
 public:
  PredicatePushdownTest() {
    nblocks_ = 10;
    nrows_ = 1000000; 
  }

  virtual void SetUp() OVERRIDE {
    CFileTestBase::SetUp();
  }
  
  void CardinalityBenchmark(int cardinality) {
    Slice lower("00000000", 8);
    Slice upper("00000020", 8);
    auto pred = ColumnPredicate::Range(ColumnSchema("string_val", STRING), &lower, &upper);

    for (int i = 0; i < nblocks_; i++) {
      BlockId block_id;
      CardinalityDataGenerator generator("%08" PRId64, cardinality);

      // WriteDictBlock()
      // Creates a new block with the proper block types and compression, returning &block_id
      WriteTestFile(&generator, DICT_ENCODING, NO_COMPRESSION, nrows_, NO_FLAGS, &block_id);

      // Setup for the reader:
      //   1. Open the physical block on disk
      //   2. Open a CFileReader object on that block
      //   3. Generate a new iterator for that reader (new CFileIterator)
      //   4. Seek
      gscoped_ptr<fs::ReadableBlock> source;
      fs_manager_->OpenBlock(block_id, &source);
      gscoped_ptr<CFileReader> reader;
      ASSERT_OK(CFileReader::Open(std::move(source), ReaderOptions(), &reader));
      gscoped_ptr<CFileIterator> iter;
      ASSERT_OK(reader->NewIterator(&iter, CFileReader::DONT_CACHE_BLOCK));
      
      // Specifying to only read 100 rows at a time
      ScopedColumnBlock<STRING> cb(nrows_);
      SelectionVector *sel = new SelectionVector(nrows_);
      sel->SetAllFalse();
      bool eval_complete = false;
      ColumnEvalContext *ctx = new ColumnEvalContext(pred, &cb, sel, eval_complete);


      // initialize cb's selectionvector and empty it
      while(iter->HasNext()) {
        LOG_TIMING(INFO, "reading strings with cardinality constraint") {
          // size_t n = cb.nrows();
          iter->PrepareBatch(&nrows_);
          ASSERT_OK_FAST(iter->Scan(ctx));
          iter->FinishBatch();
        }
        LOG(INFO) << "Number selected: " << sel->CountSelected();
      }
    }
  }
  
 private:
  size_t nrows_;
  size_t nblocks_;
};

// @andrwng
// #ifdef NDEBUG
TEST_P(PredicatePushdownTest, LowCardinalityBenchmark) {
  // Create a low cardinality (10 unique values) dictionary encoded block of data

  // Create a column predicate and try to evaluate it using EvaluatePredicate
  // DoPredicateEvaluation()
  // Run multiple times on the same block size in order to obtain an estimated average

  // Preconditions
  // SelectionVector is set to the size of the data, which is n x number of blocks
  LOG(INFO) << "Runnning CardinalityBenchmark 100";
  CardinalityBenchmark(1000);
}



// INSTANTIATE_TEST_CASE_P(AllMemory, PredicatePushdownTest, ::testing::Values(ALL_IN_MEMORY));
// INSTANTIATE_TEST_CASE_P(SplitMemoryDisk, PredicatePushdownTest, ::testing::Values(SPLIT_MEMORY_DISK));
// INSTANTIATE_TEST_CASE_P(AllDisk, PredicatePushdownTest, ::testing::Values(ALL_ON_DISK));
// INSTANTIATE_TEST_CASE_P(AllDisk, EvalPredicatePushdown, ::testing::Values(ALL_ON_DISK));
} // namespace cfile
} // namespace kudu
