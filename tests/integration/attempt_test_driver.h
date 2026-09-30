#pragma once

#include "attempt_test_control.h"

#include <algorithm>
#include <chrono>
#include <condition_variable>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <stdexcept>
#include <thread>
#include <vector>

namespace artc::rpc::testing {

class AttemptTestDriver final {
 public:
  AttemptTestDriver() : control_(std::make_shared<AttemptControl>()) {
    control_->now = [this] { return clock_now(); };
    control_->arm_timer = [this](TimerKey key, control::SteadyTime due,
                                 std::function<void(bool)> callback) {
      arm(key, due, std::move(callback));
    };
    control_->cancel_timer = [this](TimerKey key) { cancel(key); };
    control_->checkpoint = [this](Checkpoint point, AttemptKind kind) {
      checkpoint(point, kind);
    };
  }

  AttemptTestDriver(const AttemptTestDriver&) = delete;
  AttemptTestDriver& operator=(const AttemptTestDriver&) = delete;

  [[nodiscard]] std::shared_ptr<AttemptControl> control() const { return control_; }

  [[nodiscard]] control::SteadyTime clock_now() const {
    std::lock_guard lock(mutex_);
    return std::max(clock_now_, control::SteadyClock::now());
  }

  void advance_to(TimerKey key) {
    control::SteadyTime due;
    {
      std::lock_guard lock(mutex_);
      due = find_timer_locked(key).due;
      clock_now_ = std::max(clock_now_, due);
    }
    changed_.notify_all();
  }

  [[nodiscard]] std::optional<TimerKey> wait_for_timer(
      TimerKind kind, std::chrono::milliseconds timeout =
                          std::chrono::seconds(2)) {
    std::unique_lock lock(mutex_);
    const auto ready = [&] {
      return std::any_of(timers_.begin(), timers_.end(), [&](const Timer& timer) {
        return timer.key.kind == kind && !timer.cancelled && !timer.delivered;
      });
    };
    if (!changed_.wait_for(lock, timeout, ready)) return std::nullopt;
    for (const auto& timer : timers_) {
      if (timer.key.kind == kind && !timer.cancelled && !timer.delivered) {
        return timer.key;
      }
    }
    return std::nullopt;
  }

  void deliver(TimerKey key, bool ok = true) {
    std::function<void(bool)> callback;
    {
      std::lock_guard lock(mutex_);
      auto& timer = find_timer_locked(key);
      if (timer.delivered) return;
      timer.delivered = true;
      callback = timer.callback;
    }
    changed_.notify_all();
    callback(ok);
  }

  [[nodiscard]] std::thread deliver_async(TimerKey key, bool ok = true) {
    return std::thread([this, key, ok] { deliver(key, ok); });
  }

  void pause_next(Checkpoint point, std::optional<AttemptKind> kind = std::nullopt) {
    std::lock_guard lock(mutex_);
    if (pause_point_) {
      throw std::logic_error("attempt test checkpoint pause is already armed");
    }
    pause_point_ = point;
    pause_kind_ = kind;
    pause_reached_ = false;
    pause_released_ = false;
  }

  [[nodiscard]] bool wait_until_paused(std::chrono::milliseconds timeout =
                                           std::chrono::seconds(2)) {
    std::unique_lock lock(mutex_);
    return changed_.wait_for(lock, timeout, [&] { return pause_reached_; });
  }

  void release_pause() {
    {
      std::lock_guard lock(mutex_);
      pause_released_ = true;
    }
    changed_.notify_all();
  }

  [[nodiscard]] std::size_t checkpoint_count(Checkpoint point) const {
    std::lock_guard lock(mutex_);
    return static_cast<std::size_t>(std::count_if(
        checkpoints_.begin(), checkpoints_.end(),
        [point](const auto& event) { return event.first == point; }));
  }

  [[nodiscard]] bool wait_for_checkpoint_count(
      Checkpoint point, std::size_t count,
      std::chrono::milliseconds timeout = std::chrono::seconds(2)) {
    std::unique_lock lock(mutex_);
    return changed_.wait_for(lock, timeout, [&] {
      return static_cast<std::size_t>(std::count_if(
                 checkpoints_.begin(), checkpoints_.end(),
                 [point](const auto& event) { return event.first == point; })) >= count;
    });
  }

  [[nodiscard]] std::size_t active_timers() const {
    std::lock_guard lock(mutex_);
    return static_cast<std::size_t>(std::count_if(
        timers_.begin(), timers_.end(), [](const Timer& timer) {
          return !timer.cancelled && !timer.delivered;
        }));
  }

 private:
  struct Timer {
    TimerKey key;
    control::SteadyTime due{};
    std::function<void(bool)> callback;
    bool cancelled{false};
    bool delivered{false};
  };

  void arm(TimerKey key, control::SteadyTime due,
           std::function<void(bool)> callback) {
    {
      std::lock_guard lock(mutex_);
      timers_.push_back({key, due, std::move(callback)});
    }
    changed_.notify_all();
  }

  void cancel(TimerKey key) {
    {
      std::lock_guard lock(mutex_);
      for (auto& timer : timers_) {
        if (timer.key == key) {
          timer.cancelled = true;
          break;
        }
      }
    }
    changed_.notify_all();
  }

  void checkpoint(Checkpoint point, AttemptKind kind) {
    std::unique_lock lock(mutex_);
    checkpoints_.emplace_back(point, kind);
    const bool should_pause = pause_point_ == point &&
                              (!pause_kind_ || *pause_kind_ == kind);
    if (should_pause) pause_reached_ = true;
    changed_.notify_all();
    if (should_pause) {
      changed_.wait(lock, [&] { return pause_released_; });
      pause_point_.reset();
      pause_kind_.reset();
    }
  }

  Timer& find_timer_locked(TimerKey key) {
    for (auto& timer : timers_) {
      if (timer.key == key) return timer;
    }
    throw std::out_of_range("attempt test timer key not found");
  }

  mutable std::mutex mutex_;
  std::condition_variable changed_;
  control::SteadyTime clock_now_{control::SteadyClock::now()};
  std::shared_ptr<AttemptControl> control_;
  std::vector<Timer> timers_;
  std::vector<std::pair<Checkpoint, AttemptKind>> checkpoints_;
  std::optional<Checkpoint> pause_point_;
  std::optional<AttemptKind> pause_kind_;
  bool pause_reached_{false};
  bool pause_released_{false};
};

}  // namespace artc::rpc::testing
