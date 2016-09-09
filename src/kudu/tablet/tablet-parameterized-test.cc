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
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include <glog/logging.h>
#include <gtest/gtest.h>

#include <kudu/util/flags.h>

#include "kudu/common/schema.h"
#include "kudu/tablet/tablet-test-base.h"

namespace kudu {
namespace tablet {

enum Setup {
  DBGEN,
  ARTIFICIAL
};

DEFINE_int32(tablet_param_test_nrows, 100000, "Number of rows to run in single run");
DEFINE_int32(tablet_param_test_cardinality, 100, "Cardinality of column for single run");
DEFINE_int32(tablet_param_test_nrepeats, 1, "Number of times to repeat per tablet");
DEFINE_int32(tablet_param_test_lower, 0, "Lower bound on the predicate [0, p)");
DEFINE_int32(tablet_param_test_upper, 15, "Upper bound on the predicate [0, p)");
DEFINE_int32(tablet_param_test_strlen, 15, "Number of characters per string");

// TODO: put this into a base class and have tablet-decoder-eval-test extend it
class TabletParameterizedTest : public KuduTabletTest,
                                public ::testing::WithParamInterface<Setup> {
public:
  TabletParameterizedTest()
          : KuduTabletTest(Schema({ColumnSchema("key", INT32),
                                   ColumnSchema("int_val", INT32),
                                   ColumnSchema("string_val", STRING, true, NULL, NULL,
                                                ColumnStorageAttributes(DICT_ENCODING,
                                                                        DEFAULT_COMPRESSION))}, 1))
  {}

  void SetUp() override {
    KuduTabletTest::SetUp();
  }

  void TestScanAndFilter(size_t cardinality, size_t lower, size_t upper) {
    // The correctness check of this test requires that the int and string
    // comparators for the values in the tablets match up. Adjust the lengths
    // of the strings to enforce this.
    // e.g. scanning ["00", ..., "99"] for >"111" would return numerically
    // incorrect values, but ["000", ..., "099"] would return correct values.
    size_t strlen = std::max({static_cast<size_t>(FLAGS_tablet_param_test_strlen),
                              Substitute("$0", upper).length(),
                              Substitute("$0", cardinality).length()});
    FillTestTablet(cardinality, strlen);
    for (int i = 0; i < FLAGS_tablet_param_test_nrepeats; i++) {
      TestTimedScanWithBounds(cardinality, strlen, lower, upper);
    }
  }

  size_t ExpectedCount(size_t nrows, size_t cardinality, size_t lower, size_t upper) {
    if (lower >= upper || lower >= cardinality) {
      return 0;
    }
    lower = std::max(static_cast<size_t>(0), lower);
    upper = std::min(cardinality, upper);
    size_t last_chunk_count = 0;
    size_t last_chunk_size = nrows % cardinality;
    if (last_chunk_size == 0 || last_chunk_size <= lower) {
      // Lower: 3, upper: 8, cardinality:10, nrows: 23, last_chunk_size: 3
      // [0001111100|0001111100|000]
      last_chunk_count = 0;
    } else if (last_chunk_size <= upper) {
      // Lower: 3, upper: 8, cardinality:10, nrows: 25, last_chunk_size: 5
      // [0001111100|0001111100|00011]
      last_chunk_count = last_chunk_size - lower;
    } else {
      // Lower: 3, upper: 8, cardinality:10, nrows: 29, last_chunk_size: 9
      // [0001111100|0001111100|000111110]
      last_chunk_count = upper - lower;
    }
    return (nrows / cardinality) * (upper - lower) + last_chunk_count;
  }

  void FillTestTablet(size_t cardinality, size_t strlen) {
    size_t nrows = static_cast<size_t>(FLAGS_tablet_param_test_nrows);
    RowBuilder rb(client_schema_);

    LocalTabletWriter writer(tablet().get(), &client_schema_);
    KuduPartialRow row(&client_schema_);

    // Fill tablet with repeated pattern of strings.
    // e.g. (Cardinality: 3, strlen: 2): "00", "01", "02", "00", "01", "02", ...
    for (int64_t i = 0; i < nrows; i++) {
      CHECK_OK(row.SetInt32(0, i));
      CHECK_OK(row.SetInt32(1, i * 10));
      CHECK_OK(row.SetStringCopy(2, StringPrintf(
              Substitute("%0$0$1", strlen, PRId64).c_str(),
              i%cardinality)));

      ASSERT_OK_FAST(writer.Insert(row));
    }
    ASSERT_OK(tablet()->Flush());
  }

  void TestTimedScanWithBounds(size_t cardinality, size_t strlen, size_t lower_val,
                               size_t upper_val) {
    size_t nrows = static_cast<size_t>(FLAGS_tablet_param_test_nrows);
    Arena arena(128, 1028);
    AutoReleasePool pool;
    ScanSpec spec;
    const std::string lower_string =
            StringPrintf(Substitute("%0$0$1", strlen, PRId64).c_str(),
                         static_cast<int64_t>(lower_val));
    const std::string upper_string =
            StringPrintf(Substitute("%0$0$1", strlen, PRId64).c_str(),
                         static_cast<int64_t>(upper_val));
    Slice lower(lower_string);
    Slice upper(upper_string);

    auto string_pred = ColumnPredicate::Range(schema_.column(2), &lower, &upper);
    spec.AddPredicate(string_pred);
    spec.OptimizeScan(schema_, &arena, &pool, true);
    ScanSpec orig_spec = spec;
    gscoped_ptr<RowwiseIterator> iter;
    ASSERT_OK(tablet()->NewRowIterator(client_schema_, &iter));
    spec = orig_spec;
    ASSERT_OK(iter->Init(&spec));
    ASSERT_TRUE(spec.predicates().empty()) << "Should have accepted all predicates";

    int fetched = 0;
    LOG_TIMING(INFO, "Filtering by string value") {
      ASSERT_OK(SilentIterateToStringList(iter.get(), &fetched));
    }
    size_t expected_sel_count = ExpectedCount(FLAGS_tablet_param_test_nrows,
                                              cardinality, lower_val, upper_val);
    ASSERT_EQ(expected_sel_count, fetched);
    LOG(INFO) << "Nrows: " << nrows
              << ", Strlen: " << strlen
              << ", Expected: " << expected_sel_count
              << ", Actual: " << fetched;
  }
};

TEST_P(TabletParameterizedTest, WithParams) {
  TestScanAndFilter(FLAGS_tablet_param_test_cardinality,
                    FLAGS_tablet_param_test_lower,
                    FLAGS_tablet_param_test_upper);
}

int main(int argc, char *argv[]) {
  kudu::ParseCommandLineFlags(&argc, &argv, true);
  return 0;
}

INSTANTIATE_TEST_CASE_P(ParameterizedTest, TabletParameterizedTest, ::testing::Values(ARTIFICIAL));

}   // namespace tablet
}   // namespace kudu
