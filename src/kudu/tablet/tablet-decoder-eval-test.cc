#include <glog/logging.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <vector>

#include "kudu/common/schema.h"
#include "kudu/tablet/tablet-test-base.h"
#include "kudu/tablet/tablet.h"
#include "kudu/util/auto_release_pool.h"
#include "kudu/util/memory/arena.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"

namespace kudu {
namespace tablet {

enum Setup {
  ALL_IN_MEMORY,
  SPLIT_MEMORY_DISK,
  ALL_ON_DISK
};

class TabletDecoderEvalTest : public KuduTabletTest,
                              public ::testing::WithParamInterface<Setup> {
public:
  // Creates a new tablet for every cardinality being tested
  // This is so that each can be evaluated on a dataset specific to the cardinality
  TabletDecoderEvalTest()
    : KuduTabletTest(Schema({ColumnSchema("key", INT32),
                             ColumnSchema("int_val", INT32),
                             ColumnSchema("string_val", STRING, false, NULL, NULL,
                             ColumnStorageAttributes(DICT_ENCODING, DEFAULT_COMPRESSION)) }, 1)),
      cardinalities_({5, 10, 15, 20, 30, 40, 60, 80, 100, 200, 400, 500, 1000}),
      num_tablets_(cardinalities_.size()) {
  }

  virtual void SetUp() OVERRIDE {
    KuduTabletTest::SetUp();
  }

  void FillTestTablet(size_t cardinality) {
    RowBuilder rb(client_schema_);
    nrows_ = 2100;
    if (AllowSlowTests()) {
        nrows_ = 100000;
    }
    // Create new tablet/replace old tablet
    LocalTabletWriter writer(tablet().get(), &client_schema_);
    KuduPartialRow row(&client_schema_);
    for (int64_t i = 0; i < nrows_; i++) {
      CHECK_OK(row.SetInt32(0, i));
      CHECK_OK(row.SetInt32(1, i * 10));
      CHECK_OK(row.SetStringCopy(2, StringPrintf("%08" PRId64, i % cardinality)));
      ASSERT_OK_FAST(writer.Insert(row));

      if (i == 205 && GetParam() == SPLIT_MEMORY_DISK) {
        ASSERT_OK(tablet()->Flush());
      }
    }
    if (GetParam() == ALL_ON_DISK) {
      ASSERT_OK(tablet()->Flush());
    }

  }

  // Determines the number of expected results assuming predicate of [0, pred_upper)
  // Evaluates the predicate specified by spec and compares the results
  void TestScanYieldsExpectedResults(ScanSpec spec, size_t pred_upper) {
    Arena arena(128, 1028);
    AutoReleasePool pool;
    spec.OptimizeScan(schema_, &arena, &pool, true);
    ScanSpec orig_spec = spec;

    for (size_t i = 0; i < cardinalities_.size(); i++) {
      // Open a new tablet and fill it with the correct data
      FillTestTablet(cardinalities_[i]);

      size_t expected_sel_count = pred_upper > cardinalities_[i] ?
                               nrows_ :
                               (nrows_ / cardinalities_[i]) * pred_upper + std::min(nrows_ % cardinalities_[i], pred_upper);
      gscoped_ptr<RowwiseIterator> iter;
      ASSERT_OK(tablet()->NewRowIterator(client_schema_, &iter));
      spec = orig_spec;
      ASSERT_OK(iter->Init(&spec));
      ASSERT_TRUE(spec.predicates().empty()) << "Should have accepted all predicates";

      int fetched = 0;
      LOG_TIMING(INFO, "Filtering by string value") {
        ASSERT_OK(SilentIterateToStringList(iter.get(), fetched));
      }
      LOG(INFO) << "Cardinality: " << cardinalities_[i] << ", Expected: " << expected_sel_count << ", Actual: " << fetched;
      ASSERT_EQ(expected_sel_count, fetched);

      int expected_blocks_from_disk;
      int expected_rows_from_disk;
      bool check_stats = true;
      switch (GetParam()) {
        case ALL_IN_MEMORY:
          expected_blocks_from_disk = 0;
          expected_rows_from_disk = 0;
          break;
        case SPLIT_MEMORY_DISK:
          expected_blocks_from_disk = 1;
          expected_rows_from_disk = 206;
          break;
        case ALL_ON_DISK:
          // If AllowSlowTests() is true and all data is on disk
          // (vs. first 206 rows -- containing the values we're looking
          // for -- on disk and the rest in-memory), then the number
          // of blocks and rows we will scan through can't be easily
          // determined (as it depends on default cfile block size, the
          // size of cfile header, and how much data each column takes
          // up).
          if (AllowSlowTests()) {
            check_stats = false;
          } else {
            // If AllowSlowTests() is false, then all of the data fits
            // into a single cfile.
            expected_blocks_from_disk = 1;
            expected_rows_from_disk = nrows_;
          }
          break;
      }
      if (check_stats) {
        vector<IteratorStats> stats;
        iter->GetIteratorStats(&stats);
        for (const IteratorStats &col_stats : stats) {
          EXPECT_EQ(expected_blocks_from_disk, col_stats.data_blocks_read_from_disk);
          EXPECT_EQ(expected_rows_from_disk, col_stats.cells_read_from_disk);
        }
      }
    }
  }

  // Test that a scan with an empty projection and the given spec
  // returns the expected number of rows. The rows themselves
  // should be empty.
  void TestCountOnlyScanYieldsExpectedResults(ScanSpec spec) {
    Arena arena(128, 1028);
    AutoReleasePool pool;
    spec.OptimizeScan(schema_, &arena, &pool, true);

    Schema empty_schema(std::vector<ColumnSchema>(), 0);
    gscoped_ptr <RowwiseIterator> iter;
    ASSERT_OK(tablet()->NewRowIterator(empty_schema, &iter));
    ASSERT_OK(iter->Init(&spec));
    ASSERT_TRUE(spec.predicates().empty()) << "Should have accepted all predicates";

    vector <string> results;
    ASSERT_OK(IterateToStringList(iter.get(), &results));
    ASSERT_EQ(11, results.size());
    for (const string &result : results) {
        ASSERT_EQ("()", result);
    }
  }

private:
  const std::vector<const size_t> cardinalities_;
  size_t num_tablets_;
  size_t nrows_;
};


TEST_P(TabletDecoderEvalTest, TestMultiTabletBenchmark) {
  // Push down a double-ended range on the integer value column.

  ScanSpec spec;
  const std::string lower_string = StringPrintf("%08" PRId64, static_cast<int64_t>(0));
  const std::string upper_string = StringPrintf("%08" PRId64, static_cast<int64_t>(21));
  Slice lower(lower_string);
  Slice upper(upper_string);

  auto string_pred = ColumnPredicate::Range(schema_.column(2), &lower, &upper);
  spec.AddPredicate(string_pred);

  // Performs a scan on the data with bounds string( [0,21) )
  TestScanYieldsExpectedResults(spec, 21);

  // TODO: support non-key predicate pushdown on columns which aren't
  // part of the projection. The following line currently would crash.
  // TestCountOnlyScanYieldsExpectedResults(spec);

  // TODO: collect IO statistics per column, verify that most of the string blocks
  // were not read.
}

TEST_P(TabletDecoderEvalTest, TestMultiTabletBenchmarkNext) {
  // Push down a double-ended range on the integer value column.

  ScanSpec spec;
  const std::string lower_string = StringPrintf("%08" PRId64, static_cast<int64_t>(0));
  const std::string upper_string = StringPrintf("%08" PRId64, static_cast<int64_t>(21));
  Slice lower(lower_string);
  Slice upper(upper_string);

  auto string_pred = ColumnPredicate::Range(schema_.column(2), &lower, &upper);
  spec.AddPredicate(string_pred);

  // Performs a scan on the data with bounds string( [0,21) )
  TestScanYieldsExpectedResults(spec, 22);

  // TODO: support non-key predicate pushdown on columns which aren't
  // part of the projection. The following line currently would crash.
  // TestCountOnlyScanYieldsExpectedResults(spec);

  // TODO: collect IO statistics per column, verify that most of the string blocks
  // were not read.
}


INSTANTIATE_TEST_CASE_P(AllDisk, TabletDecoderEvalTest, ::testing::Values(ALL_ON_DISK));


}   // tablet
}   // kudu