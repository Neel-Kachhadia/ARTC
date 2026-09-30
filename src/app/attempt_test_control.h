#pragma once

#if defined(ARTC_ENABLE_TEST_HOOKS)

#include "artc/control/phase2.h"
#include "artc/rpc/attempt_policy.h"

#include <cstdint>
#include <functional>

namespace artc::rpc::testing {

enum class TimerKind : std::uint8_t { kHedge, kRetry, kDeadline };

struct TimerKey {
  std::uint64_t manager_id{0};
  TimerKind kind{TimerKind::kHedge};
  std::uint64_t generation{0};

  friend bool operator==(const TimerKey&, const TimerKey&) = default;
};

enum class Checkpoint : std::uint8_t {
  kHedgeTimerReady,
  kRetryTimerReady,
  kDeadlineTimerReady,
  kBeforeDispatchFence,
  kInsideDispatchFence,
  kShutdownFenceContended,
  kShutdownFenceClosed,
};

struct AttemptControl {
  std::function<control::SteadyTime()> now;
  std::function<void(TimerKey, control::SteadyTime,
                     std::function<void(bool)>)> arm_timer;
  std::function<void(TimerKey)> cancel_timer;
  std::function<void(Checkpoint, AttemptKind)> checkpoint;
};

}  // namespace artc::rpc::testing

#endif
