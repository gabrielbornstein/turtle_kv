#include <turtle_kv/util/piecewise_filter.hpp>
//
#include <turtle_kv/util/piecewise_filter.hpp>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <turtle_kv/core/testing/generate.hpp>

#include <boost/range/algorithm/equal_range.hpp>

#include <algorithm>
#include <random>
#include <string>
#include <string_view>
#include <type_traits>
#include <unordered_set>
#include <vector>

namespace {

using namespace turtle_kv::int_types;

using turtle_kv::CInterval;
using turtle_kv::Interval;
using turtle_kv::None;
using turtle_kv::Optional;
using turtle_kv::PiecewiseFilter;
using turtle_kv::Slice;
using turtle_kv::Status;
using turtle_kv::StatusOr;
using turtle_kv::testing::RandomStringGenerator;

using llfs::KeyRangeOrder;
using llfs::StableStringStore;

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
TEST(PiecewiseFilterTest, DroppedTotalIncreaseTest)
{
  usize total_items = 80;
  PiecewiseFilter<usize> filter;

  EXPECT_EQ(filter.dropped_total(), 0u);

  filter.drop_index_range(Interval<usize>{0, 9}, total_items);
  EXPECT_EQ(filter.dropped_total(), 9u);

  filter.drop_index_range(Interval<usize>{34, 80}, total_items);
  EXPECT_EQ(filter.dropped_total(), 55u);

  filter.drop_index_range(Interval<usize>{30, 85}, total_items);
  EXPECT_EQ(filter.dropped_total(), 59u);

  filter.drop_index_range(Interval<usize>{0, 80}, total_items);
  EXPECT_EQ(filter.dropped_total(), 80u);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
TEST(PiecewiseFilterTest, InvalidFilterTest)
{
  // Interval starting with zero twice.
  //
  std::vector<Interval<usize>> dropped{Interval<usize>{0, 10},
                                       Interval<usize>{20, 30},
                                       Interval<usize>{0, 40}};
  StatusOr<PiecewiseFilter<usize>> filter = PiecewiseFilter<usize>::from_dropped(as_slice(dropped));
  EXPECT_FALSE(filter.ok());
  EXPECT_EQ(filter.status(), Status{::batt::StatusCode::kInvalidArgument});

  // Overlapping intervals.
  //
  dropped = {Interval<usize>{0, 20}, Interval<usize>{10, 25}};
  filter = PiecewiseFilter<usize>::from_dropped(as_slice(dropped));
  EXPECT_FALSE(filter.ok());
  EXPECT_EQ(filter.status(), Status{::batt::StatusCode::kInvalidArgument});

  // Backward interval.
  //
  dropped = {Interval<usize>{0, 30}, Interval<usize>{50, 40}};
  filter = PiecewiseFilter<usize>::from_dropped(as_slice(dropped));
  EXPECT_FALSE(filter.ok());
  EXPECT_EQ(filter.status(), Status{::batt::StatusCode::kInvalidArgument});
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
TEST(PiecewiseFilterTest, CutPointsSizeTest)
{
  usize total_items = 80;
  PiecewiseFilter<usize> filter;

  // No drops yet, so we have no cut points.
  //
  EXPECT_EQ(filter.cut_points_size(), 0u);

  // Filter out the first 10 items to create one cut point.
  //
  filter.drop_index_range(Interval<usize>{0, 10}, total_items);
  EXPECT_EQ(filter.cut_points_size(), 1u);

  // Filter out another 10 items with an Interval adjacent to the previous one, which will still
  // create one cut point since the Intervals will be merged.
  //
  filter.drop_index_range(Interval<usize>{10, 20}, total_items);
  EXPECT_EQ(filter.cut_points_size(), 1u);

  // Filter out some items that in an Interval that is not adjacent to what is currently filtered.
  // This will create two more cut points on top of the already existing one, since we need to
  // track index 30 and 70.
  //
  filter.drop_index_range(Interval<usize>{30, 70}, total_items);
  EXPECT_EQ(filter.cut_points_size(), 3u);

  // Drop an interval that is not adjacent to the previous interval but covers the last item.
  //
  filter.drop_index_range(Interval<usize>{75, 80}, total_items);
  EXPECT_EQ(filter.cut_points_size(), 5u);

  // Drop all the remaining items, resulting in one cut point.
  //
  filter.drop_index_range(Interval<usize>{70, 75}, total_items);
  filter.drop_index_range(Interval<usize>{20, 30}, total_items);
  EXPECT_EQ(filter.cut_points_size(), 1u);
}

TEST(PiecewiseFilterTest, QueryTest)
{
  u32 num_keys = 1000;
  std::vector<std::string_view> keys;
  keys.reserve(num_keys);

  // Generate some random strings and sort them.
  //
  std::default_random_engine rng{/*seed=*/30};
  RandomStringGenerator generate_key;
  llfs::StableStringStore store;
  std::unordered_set<std::string_view> keys_set;
  for (u32 i = 0; i < num_keys; ++i) {
    std::string_view key = generate_key(rng, store);
    if (keys_set.contains(key)) {
      continue;
    }

    keys.emplace_back(key);
  }
  std::sort(keys.begin(), keys.end());

  PiecewiseFilter<u32> filter;

  auto drop_item_range = [&keys, &filter](const auto& item_range) {
    auto iters = boost::range::equal_range(keys, item_range, KeyRangeOrder{});

    u32 start_i = BATT_CHECKED_CAST(u32, std::distance(keys.begin(), iters.first));
    u32 end_i = BATT_CHECKED_CAST(u32, std::distance(keys.begin(), iters.second));

    u32 total_items = BATT_CHECKED_CAST(u32, keys.size());

    filter.drop_index_range(Interval<u32>{start_i, end_i}, total_items);
  };

  // First verify that everything should be live since nothing has been dropped yet.
  //
  EXPECT_EQ(filter.dropped_total(), 0);

  Optional<Interval<u32>> next_live_interval = filter.find_live_range(0, num_keys);
  BATT_CHECK(next_live_interval);
  EXPECT_EQ(*next_live_interval, (Interval<u32>{0, num_keys}));

  // Drop an interval in the middle of the items range, and query the filter.
  //
  CInterval<std::string_view> cinterval_dropped{keys[100], keys[300]};
  drop_item_range(cinterval_dropped);

  EXPECT_EQ(filter.dropped_total(), 201);

  EXPECT_TRUE(filter.live_at_index(99));
  EXPECT_TRUE(filter.live_at_index(301));
  EXPECT_FALSE(filter.live_at_index(100));
  EXPECT_FALSE(filter.live_at_index(200));
  EXPECT_FALSE(filter.live_at_index(300));

  Optional<u32> next_live_index = filter.live_lower_bound(100, num_keys);
  EXPECT_TRUE(next_live_index);
  EXPECT_EQ(*next_live_index, 301);

  next_live_interval = filter.find_live_range(0, num_keys);
  BATT_CHECK(next_live_interval);
  EXPECT_EQ(*next_live_interval, (Interval<u32>{0, 100}));

  next_live_interval = filter.find_live_range(301, num_keys);
  BATT_CHECK(next_live_interval);
  EXPECT_EQ(*next_live_interval, (Interval<u32>{301, num_keys}));

  // When next_live_interval is called with a filtered starting index, the returned interval
  // should be empty.
  //
  EXPECT_EQ(filter.find_live_range(100, num_keys), None);

  // Drop another interval that is not adjacent to the previously dropped one.
  //
  Interval<std::string_view> interval_dropped{keys[600], keys.back()};
  drop_item_range(interval_dropped);

  EXPECT_TRUE(filter.live_at_index(num_keys - 1));
  EXPECT_TRUE(filter.live_at_index(400));
  EXPECT_FALSE(filter.live_at_index(600));
  EXPECT_FALSE(filter.live_at_index(num_keys - 2));

  next_live_index = filter.live_lower_bound(700, num_keys);
  EXPECT_TRUE(next_live_index);
  EXPECT_EQ(*next_live_index, num_keys - 1);

  next_live_index = filter.live_lower_bound(num_keys - 1, num_keys);
  EXPECT_TRUE(next_live_index);
  EXPECT_EQ(*next_live_index, num_keys - 1);

  next_live_interval = filter.find_live_range(num_keys - 1, num_keys);
  BATT_CHECK(next_live_interval);
  EXPECT_EQ(*next_live_interval, (Interval<u32>{num_keys - 1, num_keys}));

  next_live_interval = filter.find_live_range(301, num_keys);
  BATT_CHECK(next_live_interval);
  EXPECT_EQ(next_live_interval, (Interval<u32>{301, 600}));

  // Drop another range in the middle, this time with overlap until the end.
  //
  cinterval_dropped = CInterval<std::string_view>{keys[500], keys[num_keys - 1]};
  drop_item_range(cinterval_dropped);

  EXPECT_FALSE(filter.live_at_index(num_keys - 1));
  EXPECT_TRUE(filter.live_at_index(301));

  next_live_index = filter.live_lower_bound(500, num_keys);
  EXPECT_FALSE(next_live_index);

  next_live_interval = filter.find_live_range(301, num_keys);
  BATT_CHECK(next_live_interval);
  EXPECT_EQ(*next_live_interval, (Interval<u32>{301, 500}));

  // Drop everything.
  //
  filter.drop_index_range(Interval<u32>{0, num_keys}, num_keys);

  EXPECT_EQ(filter.dropped_total(), num_keys);

  EXPECT_FALSE(filter.live_at_index(0));
  next_live_index = filter.live_lower_bound(0, num_keys);
  EXPECT_FALSE(next_live_index);

  EXPECT_EQ(filter.find_live_range(0, num_keys), None);
}
}  // namespace
