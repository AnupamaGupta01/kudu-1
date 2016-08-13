#include <glog/logging.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <vector>
#include <kudu/util/flags.h>

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

// These are the default values
DEFINE_int32(nrows, 1000000, "Number of rows generated per tablet");
DEFINE_int32(cardinality, 66666, "Cardinality of the string column");
DEFINE_int32(strlen, 64, "Length of each word in the string column");
DEFINE_int32(pred_upper, 21, "Upper bound on the predicate [0, p)");
DEFINE_int32(nrepeats, 1, "Number of times to repeat per tablet");
DEFINE_bool(pushdown, true, "Flag to switch on/off pred pushdown");


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

  void FillTestTablet() {
    RowBuilder rb(client_schema_);

    // Create new tablet/replace old tablet
    LocalTabletWriter writer(tablet().get(), &client_schema_);
    KuduPartialRow row(&client_schema_);
    for (int64_t i = 0; i < FLAGS_nrows; i++) {
      CHECK_OK(row.SetInt32(0, i));
      CHECK_OK(row.SetInt32(1, i * 10));
      CHECK_OK(row.SetStringCopy(2, StringPrintf(("%0"+std::to_string(FLAGS_strlen) + PRId64).c_str(),
                                                 i%FLAGS_cardinality)));
      ASSERT_OK_FAST(writer.Insert(row));

      if (i == 205 && GetParam() == SPLIT_MEMORY_DISK) {
        ASSERT_OK(tablet()->Flush());
      }
    }
    if (GetParam() == ALL_ON_DISK) {
      ASSERT_OK(tablet()->Flush());
    }

  }
  void SetupTablet() {
  }
  void TestTimedScanAndFilter() {

    Arena arena(128, 1028);
    AutoReleasePool pool;
    ScanSpec spec;
    const std::string lower_string = StringPrintf(("%0"+std::to_string(FLAGS_strlen) + PRId64).c_str(),
                                                  static_cast<int64_t>(0));
    const std::string upper_string = StringPrintf(("%0"+std::to_string(FLAGS_strlen) + PRId64).c_str(),
                                                  static_cast<int64_t>(FLAGS_pred_upper));
    Slice lower(lower_string);
    Slice upper(upper_string);

    auto string_pred = ColumnPredicate::Range(schema_.column(2), &lower, &upper);
    spec.AddPredicate(string_pred);
    spec.OptimizeScan(schema_, &arena, &pool, true);
    ScanSpec orig_spec = spec;
    size_t expected_sel_count = FLAGS_pred_upper > FLAGS_cardinality ?
                                FLAGS_nrows :
                                (FLAGS_nrows / FLAGS_cardinality) * FLAGS_pred_upper +
                                        std::min(FLAGS_nrows % FLAGS_cardinality, FLAGS_pred_upper);
    gscoped_ptr<RowwiseIterator> iter;
    ASSERT_OK(tablet()->NewRowIterator(client_schema_, &iter));
    spec = orig_spec;
    ASSERT_OK(iter->Init(&spec));
    ASSERT_TRUE(spec.predicates().empty()) << "Should have accepted all predicates";

    int fetched = 0;
    LOG_TIMING(INFO, "Filtering by string value") {
      if (FLAGS_pushdown) {
        ASSERT_OK(PushedIterateToStringList(iter.get(), fetched));
      }
      else {
        ASSERT_OK(SilentIterateToStringList(iter.get(), fetched));
      }
       
    }

    ASSERT_EQ(expected_sel_count, fetched);
    LOG(INFO) << "Nrows: " << FLAGS_nrows <<  ", Cardinality: " << FLAGS_cardinality << ", strlen: "
              << FLAGS_strlen << ", Expected: " << expected_sel_count << ", Actual: " << fetched;

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
          expected_rows_from_disk = FLAGS_nrows;
        }
        break;
    }
  }
};

TEST_P(TabletDecoderEvalTest, TestMultiTabletBenchmark) {
  FillTestTablet();
  for (int i = 0; i < FLAGS_nrepeats; i++) {
    TestTimedScanAndFilter();
  }
}

int main(int argc, char *argv[]) {
  kudu::ParseCommandLineFlags(&argc, &argv, true);
  return 0;
}

INSTANTIATE_TEST_CASE_P(AllDisk, TabletDecoderEvalTest, ::testing::Values(ALL_ON_DISK));

}   // tablet
}   // kudu