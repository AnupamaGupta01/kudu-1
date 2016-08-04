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
                             ColumnStorageAttributes(DICT_ENCODING, DEFAULT_COMPRESSION)) }, 1)) {
  }

  virtual void SetUp() OVERRIDE {
    KuduTabletTest::SetUp();
  }

  void FillTestTablet(const size_t nrows, const size_t strlen, const size_t cardinality) {
    RowBuilder rb(client_schema_);

    // Create new tablet/replace old tablet
    LocalTabletWriter writer(tablet().get(), &client_schema_);
    KuduPartialRow row(&client_schema_);
    for (int64_t i = 0; i < nrows; i++) {
      CHECK_OK(row.SetInt32(0, i));
      CHECK_OK(row.SetInt32(1, i * 10));
      CHECK_OK(row.SetStringCopy(2, StringPrintf(("%0"+std::to_string(strlen) + PRId64).c_str(), i % cardinality)));
      ASSERT_OK_FAST(writer.Insert(row));

      if (i == 205 && GetParam() == SPLIT_MEMORY_DISK) {
        ASSERT_OK(tablet()->Flush());
      }
    }
    if (GetParam() == ALL_ON_DISK) {
      ASSERT_OK(tablet()->Flush());
    }

  }
  void TestTimedScanAndFilter(const size_t nrows, const size_t pred_upper, const size_t strlen, const size_t cardinality) {
    Arena arena(128, 1028);
    AutoReleasePool pool;
    ScanSpec spec;
    const std::string lower_string = StringPrintf(("%0"+std::to_string(strlen) + PRId64).c_str(), static_cast<int64_t>(0));
    const std::string upper_string = StringPrintf(("%0"+std::to_string(strlen) + PRId64).c_str(), static_cast<int64_t>(21));
    Slice lower(lower_string);
    Slice upper(upper_string);

    auto string_pred = ColumnPredicate::Range(schema_.column(2), &lower, &upper);
    spec.AddPredicate(string_pred);
    spec.OptimizeScan(schema_, &arena, &pool, true);
    ScanSpec orig_spec = spec;
    FillTestTablet(nrows, strlen, cardinality);
    size_t expected_sel_count = pred_upper > cardinality ?
                                nrows :
                                (nrows / cardinality) * pred_upper + std::min(nrows % cardinality, pred_upper);
    gscoped_ptr<RowwiseIterator> iter;
    ASSERT_OK(tablet()->NewRowIterator(client_schema_, &iter));
    spec = orig_spec;
    ASSERT_OK(iter->Init(&spec));
    ASSERT_TRUE(spec.predicates().empty()) << "Should have accepted all predicates";

    int fetched = 0;
    LOG_TIMING(INFO, "Filtering by string value") {
//      ASSERT_OK(SilentIterateToStringList(iter.get(), fetched));
      ASSERT_OK(PushedIterateToStringList(iter.get(), fetched));
    }
    LOG(INFO) << "Nrows: " << nrows <<  "Cardinality: " << cardinality << ", strlen: " << strlen << ", Expected: " << \
      expected_sel_count << ", Actual: " << fetched;
    ASSERT_EQ(expected_sel_count, fetched);
  }
};


TEST_P(TabletDecoderEvalTest, TestMultiTabletBenchmark) {
  // Performs a scan on the data with bounds string( [0,21) )
  std::vector<size_t> nrows = {100000};//, 5000, 10000, 50000, 100000, 500000, 1000000};
//  std::vector<size_t> cardinalities = {2, 4, 6, 8, 10, 15, 20, 30, 40, 80, 100, 200, 400, 600, 800, 1000};
  std::vector<size_t> cardinalities = {24};
  std::vector<size_t> strlens = {8, 16, 24, 32, 40, 48, 56, 64};
  for (const size_t r: nrows) {
    for (const size_t crd : cardinalities) {
      for (const size_t strlen : strlens) {
        TestTimedScanAndFilter(r, 21, strlen, crd);
        SetUpTestTablet();
      }
    }
  }
}

INSTANTIATE_TEST_CASE_P(AllDisk, TabletDecoderEvalTest, ::testing::Values(ALL_ON_DISK));

}   // tablet
}   // kudu
