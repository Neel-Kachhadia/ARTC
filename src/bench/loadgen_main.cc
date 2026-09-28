#include "artc/bench/arrival_schedule.h"
#include "artc/bench/histogram.h"
#include "artc/rpc/services.h"

#include <algorithm>
#include <charconv>
#include <chrono>
#include <cmath>
#include <condition_variable>
#include <cstdint>
#include <cstdlib>
#include <ctime>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <map>
#include <memory>
#include <mutex>
#include <set>
#include <sstream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

namespace {

using Clock = std::chrono::steady_clock;
using namespace std::chrono_literals;

struct Options {
  std::string target;
  std::string run_id{"local"};
  std::string git_revision{"unknown"};
  std::string worktree_dirty{"unknown"};
  std::string compose_project{"local"};
  std::string routing_policy{"unknown"};
  std::filesystem::path output;
  artc::bench::ArrivalMode mode{artc::bench::ArrivalMode::kConstant};
  std::string mode_name{"constant"};
  std::chrono::milliseconds duration{10s};
  double rate_rps{100.0};
  double initial_rate_rps{0.0};
  std::uint64_t seed{1};
  std::size_t max_inflight{256};
  std::uint64_t max_issue_lag_us{5'000};
  std::uint32_t work_units{1};
  std::uint32_t payload_bytes{0};
  std::chrono::milliseconds deadline{5s};
  bool invoke_dependency{false};
  bool health{false};
  bool validate_only{false};
};

template <typename T>
T parse_integer(std::string_view value, std::string_view name) {
  T result{};
  const auto [end, error] = std::from_chars(value.data(), value.data() + value.size(), result);
  if (error != std::errc{} || end != value.data() + value.size()) {
    throw std::invalid_argument("invalid value for " + std::string(name));
  }
  return result;
}

double parse_double(std::string_view value, std::string_view name) {
  double result = 0.0;
  const auto [end, error] = std::from_chars(value.data(), value.data() + value.size(), result);
  if (error != std::errc{} || end != value.data() + value.size() || !std::isfinite(result)) {
    throw std::invalid_argument("invalid value for " + std::string(name));
  }
  return result;
}

Options parse_options(int argc, char** argv) {
  Options options;
  if (const char* value = std::getenv("ARTC_RUN_ID")) options.run_id = value;
  if (const char* value = std::getenv("ARTC_GIT_REVISION")) options.git_revision = value;
  if (const char* value = std::getenv("ARTC_WORKTREE_DIRTY")) options.worktree_dirty = value;
  if (const char* value = std::getenv("ARTC_COMPOSE_PROJECT")) options.compose_project = value;
  if (const char* value = std::getenv("ARTC_ROUTING_POLICY")) options.routing_policy = value;
  for (int index = 1; index < argc; ++index) {
    const std::string_view key(argv[index]);
    if (key == "--help") {
      std::cout << "artc_loadgen --target HOST:PORT --output DIR [--mode constant|poisson|step|ramp|burst|scripted] "
                   "[--rate-rps N] [--initial-rate-rps N] [--duration-ms N] [--seed N] "
                   "[--max-inflight N] [--max-issue-lag-us N] [--deadline-ms N] "
                   "[--work-units N] [--payload-bytes N] [--invoke-dependency]\n"
                   "artc_loadgen --health --target HOST:PORT\n"
                   "artc_loadgen --validate-schedule [workload options]\n";
      std::exit(0);
    }
    if (key == "--health") {
      options.health = true;
      continue;
    }
    if (key == "--validate-schedule") {
      options.validate_only = true;
      continue;
    }
    if (key == "--invoke-dependency") {
      options.invoke_dependency = true;
      continue;
    }
    if (index + 1 >= argc) throw std::invalid_argument("missing option value");
    const std::string_view value(argv[++index]);
    if (key == "--target") options.target = value;
    else if (key == "--output") options.output = value;
    else if (key == "--mode") {
      options.mode_name = value;
      options.mode = artc::bench::parse_arrival_mode(value);
    } else if (key == "--rate-rps") options.rate_rps = parse_double(value, key);
    else if (key == "--initial-rate-rps") options.initial_rate_rps = parse_double(value, key);
    else if (key == "--duration-ms") options.duration = std::chrono::milliseconds(parse_integer<std::int64_t>(value, key));
    else if (key == "--seed") options.seed = parse_integer<std::uint64_t>(value, key);
    else if (key == "--max-inflight") options.max_inflight = parse_integer<std::size_t>(value, key);
    else if (key == "--max-issue-lag-us") options.max_issue_lag_us = parse_integer<std::uint64_t>(value, key);
    else if (key == "--work-units") options.work_units = parse_integer<std::uint32_t>(value, key);
    else if (key == "--payload-bytes") options.payload_bytes = parse_integer<std::uint32_t>(value, key);
    else if (key == "--deadline-ms") options.deadline = std::chrono::milliseconds(parse_integer<std::int64_t>(value, key));
    else throw std::invalid_argument("unknown option " + std::string(key));
  }
  return options;
}

artc::bench::ArrivalScheduleConfig schedule_config(const Options& options) {
  artc::bench::ArrivalScheduleConfig config;
  config.mode = options.mode;
  config.duration = options.duration;
  config.rate_rps = options.rate_rps;
  config.initial_rate_rps = options.initial_rate_rps;
  config.seed = options.seed;
  return config;
}

void validate_options(const Options& options) {
  if (options.target.empty() && !options.validate_only) {
    throw std::invalid_argument("--target is required");
  }
  if (options.duration <= 0ms) throw std::invalid_argument("--duration-ms must be positive");
  if (options.max_inflight == 0 || options.max_inflight > 100'000) {
    throw std::invalid_argument("--max-inflight must be in [1, 100000]");
  }
  if (options.deadline <= 0ms || options.deadline > 60s) {
    throw std::invalid_argument("--deadline-ms must be in [1, 60000]");
  }
  if (options.work_units > 10'000 || options.payload_bytes > 1'048'576) {
    throw std::invalid_argument("work request exceeds service bounds");
  }
  if (!options.health && !options.validate_only && options.output.empty()) {
    throw std::invalid_argument("--output is required for a benchmark run");
  }
}

std::string json_string(std::string_view value) {
  std::string escaped;
  escaped.reserve(value.size() + 2);
  escaped.push_back('"');
  for (const char raw_character : value) {
    const auto character = static_cast<unsigned char>(raw_character);
    if (character == '"' || character == '\\') {
      escaped.push_back('\\');
      escaped.push_back(static_cast<char>(character));
    } else if (character < 0x20U) {
      constexpr char digits[] = "0123456789abcdef";
      escaped.append("\\u00");
      escaped.push_back(digits[character >> 4U]);
      escaped.push_back(digits[character & 0x0fU]);
    } else {
      escaped.push_back(static_cast<char>(character));
    }
  }
  escaped.push_back('"');
  return escaped;
}

void write_optional(std::ostream& output, const std::optional<std::uint64_t>& value) {
  if (value) output << *value;
  else output << "null";
}

void write_optional(std::ostream& output, const std::optional<double>& value) {
  if (value) output << *value;
  else output << "null";
}

void write_summary(std::ostream& output, const artc::bench::HistogramSummary& summary) {
  output << "{\"sample_count\":" << summary.sample_count << ",\"minimum_us\":";
  write_optional(output, summary.minimum_us);
  output << ",\"maximum_us\":";
  write_optional(output, summary.maximum_us);
  output << ",\"mean_us\":";
  write_optional(output, summary.mean_us);
  output << ",\"p50_us\":";
  write_optional(output, summary.p50_us);
  output << ",\"p95_us\":";
  write_optional(output, summary.p95_us);
  output << ",\"p99_us\":";
  write_optional(output, summary.p99_us);
  output << ",\"p999_us\":";
  write_optional(output, summary.p999_us);
  output << '}';
}

class ClientCall;

struct RunState {
  explicit RunState(std::chrono::milliseconds call_deadline)
      : deadline(call_deadline) {}

  void complete(ClientCall* call, const grpc::Status& status,
                const artc::v1::WorkResponse& response, Clock::time_point scheduled);

  const std::chrono::milliseconds deadline;
  // ponytail: one per-run lock keeps callback accounting bounded; shard only if issue lag shows contention.
  std::mutex mutex;
  std::condition_variable changed;
  std::set<ClientCall*> active;
  artc::bench::Histogram latency;
  artc::bench::Histogram issue_lag;
  std::map<std::string, std::uint64_t> by_replica;
  std::uint64_t issued{0};
  std::uint64_t completed{0};
  std::uint64_t succeeded{0};
  std::uint64_t deadline_goodput{0};
  std::uint64_t errors{0};
  std::uint64_t backend_attempts{0};
  std::uint64_t max_issue_lag_us{0};
  bool histogram_error{false};
};

class ClientCall final : public grpc::ClientUnaryReactor {
 public:
  ClientCall(std::shared_ptr<RunState> state, Clock::time_point scheduled,
             const Options& options, std::uint64_t request_id)
      : state_(std::move(state)), scheduled_(scheduled) {
    request_.set_request_id(request_id);
    request_.set_work_units(options.work_units);
    request_.set_payload_bytes(options.payload_bytes);
    request_.set_invoke_dependency(options.invoke_dependency);
    context_.set_deadline(std::chrono::system_clock::now() + options.deadline);
  }

  void start(artc::v1::Traffic::Stub& stub) {
    stub.async()->Execute(&context_, &request_, &response_, this);
    StartCall();
  }

  void OnDone(const grpc::Status& status) override {
    state_->complete(this, status, response_, scheduled_);
    delete this;
  }

 private:
  friend struct RunState;
  std::shared_ptr<RunState> state_;
  Clock::time_point scheduled_;
  grpc::ClientContext context_;
  artc::v1::WorkRequest request_;
  artc::v1::WorkResponse response_;
};

void RunState::complete(ClientCall* call, const grpc::Status& status,
                        const artc::v1::WorkResponse& response,
                        Clock::time_point scheduled) {
  const auto finished = Clock::now();
  const auto elapsed = finished - scheduled;
  const auto elapsed_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(elapsed);
  std::lock_guard lock(mutex);
  if (!latency.record(elapsed_ns)) histogram_error = true;
  ++completed;
  if (status.ok()) {
    ++succeeded;
    backend_attempts += response.backend_attempt_count();
    if (std::chrono::duration_cast<std::chrono::milliseconds>(elapsed) <= deadline) {
      ++deadline_goodput;
    }
    if (!response.replica_id().empty()) ++by_replica[response.replica_id()];
  } else {
    ++errors;
  }
  active.erase(call);
  changed.notify_all();
}

void write_artifacts(const Options& options, const std::vector<std::chrono::nanoseconds>& arrivals,
                     std::uint64_t started_unix_ms, std::uint64_t elapsed_wall_ms,
                     double process_cpu_seconds, const RunState& state) {
  if (std::filesystem::exists(options.output) &&
      !std::filesystem::is_empty(options.output)) {
    throw std::invalid_argument("benchmark output directory already contains files");
  }
  std::filesystem::create_directories(options.output);
  state.latency.write_raw(options.output / "latency.hdr.csv");
  state.issue_lag.write_raw(options.output / "issue-lag.hdr.csv");

  const auto latency = state.latency.summary();
  const auto issue_lag = state.issue_lag.summary();
  const bool saturated = state.max_issue_lag_us > options.max_issue_lag_us;
  const bool valid = !saturated && !state.histogram_error &&
                     state.issued == arrivals.size() && state.completed == state.issued &&
                     state.errors == 0;
  const double offered_rps = static_cast<double>(arrivals.size()) /
                             std::chrono::duration<double>(options.duration).count();
  const double amplification = state.issued == 0
                                   ? 0.0
                                   : static_cast<double>(state.backend_attempts) /
                                         static_cast<double>(state.issued);
  const double cpu_percent = elapsed_wall_ms == 0
                                 ? 0.0
                                 : process_cpu_seconds * 100'000.0 /
                                       static_cast<double>(elapsed_wall_ms);
  const double cpu_seconds_per_request = state.completed == 0
                                             ? 0.0
                                             : process_cpu_seconds /
                                                   static_cast<double>(state.completed);
  std::ostringstream manifest;
  manifest << std::setprecision(8)
           << "{\n\"schema\":\"artc-run-v1\",\n\"started_unix_ms\":" << started_unix_ms
           << ",\n\"elapsed_wall_ms\":" << elapsed_wall_ms
           << ",\n\"run_id\":" << json_string(options.run_id)
           << ",\n\"git_revision\":" << json_string(options.git_revision)
           << ",\n\"worktree_dirty\":" << json_string(options.worktree_dirty)
           << ",\n\"compose_project\":" << json_string(options.compose_project)
           << ",\n\"routing_policy\":" << json_string(options.routing_policy)
           << ",\n\"target\":" << json_string(options.target)
           << ",\n\"mode\":" << json_string(options.mode_name)
           << ",\n\"seed\":" << options.seed
           << ",\n\"duration_ms\":" << options.duration.count()
           << ",\n\"offered_rps\":" << offered_rps
           << ",\n\"scheduled\":" << arrivals.size()
           << ",\n\"issued\":" << state.issued
           << ",\n\"completed\":" << state.completed
           << ",\n\"successful\":" << state.succeeded
           << ",\n\"deadline_goodput\":" << state.deadline_goodput
           << ",\n\"errors\":" << state.errors
           << ",\n\"backend_attempts\":" << state.backend_attempts
           << ",\n\"attempt_amplification\":" << amplification
           << ",\n\"max_issue_lag_us\":" << state.max_issue_lag_us
           << ",\n\"max_issue_lag_limit_us\":" << options.max_issue_lag_us
           << ",\n\"generator_saturated\":" << (saturated ? "true" : "false")
           << ",\n\"valid\":" << (valid ? "true" : "false")
           << ",\n\"process_cpu_seconds\":" << process_cpu_seconds
           << ",\n\"process_cpu_percent\":" << cpu_percent
           << ",\n\"cpu_seconds_per_completed_request\":" << cpu_seconds_per_request
           << ",\n\"latency_us\":";
  write_summary(manifest, latency);
  manifest << ",\n\"issue_lag_us\":";
  write_summary(manifest, issue_lag);
  manifest << ",\n\"responses_by_replica\":{";
  bool first = true;
  for (const auto& [replica, count] : state.by_replica) {
    if (!first) manifest << ',';
    first = false;
    manifest << json_string(replica) << ':' << count;
  }
  manifest << "}\n}\n";

  std::ofstream output(options.output / "manifest.json", std::ios::out | std::ios::trunc);
  if (!output) throw std::runtime_error("cannot create run manifest");
  output << manifest.str();
  output.flush();
  if (!output) throw std::runtime_error("failed to write run manifest");
  std::cout << manifest.str();
}

int run(const Options& options) {
  validate_options(options);
  if (options.health) {
    std::string component;
    if (!artc::rpc::check_health(options.target, &component)) {
      std::cerr << "not ready: " << options.target << '\n';
      return 1;
    }
    std::cout << "ready " << component << '\n';
    return 0;
  }

  const auto arrivals = artc::bench::make_arrival_schedule(schedule_config(options));
  if (options.validate_only) {
    std::cout << "schedule_valid=true\nmode=" << options.mode_name
              << "\nscheduled=" << arrivals.size() << '\n';
    return 0;
  }

  auto channel = grpc::CreateChannel(options.target, grpc::InsecureChannelCredentials());
  auto stub = artc::v1::Traffic::NewStub(channel);
  auto state = std::make_shared<RunState>(options.deadline);
  const auto started = Clock::now();
  const auto started_unix_ms = static_cast<std::uint64_t>(
      std::chrono::duration_cast<std::chrono::milliseconds>(
          std::chrono::system_clock::now().time_since_epoch())
          .count());
  const std::clock_t cpu_start = std::clock();

  for (std::size_t index = 0; index < arrivals.size(); ++index) {
    const auto scheduled = started + arrivals[index];
    std::this_thread::sleep_until(scheduled);
    auto* call = new ClientCall(state, scheduled, options, static_cast<std::uint64_t>(index + 1));
    {
      std::unique_lock lock(state->mutex);
      state->changed.wait(lock, [&] { return state->active.size() < options.max_inflight; });
      const auto issued_at = Clock::now();
      const auto lag = std::chrono::duration_cast<std::chrono::nanoseconds>(issued_at - scheduled);
      if (!state->issue_lag.record(lag)) state->histogram_error = true;
      const auto lag_us = static_cast<std::uint64_t>(
          std::chrono::duration_cast<std::chrono::microseconds>(lag).count());
      state->max_issue_lag_us = std::max(state->max_issue_lag_us, lag_us);
      state->active.insert(call);
      ++state->issued;
    }
    call->start(*stub);
  }

  {
    std::unique_lock lock(state->mutex);
    state->changed.wait(lock, [&] { return state->active.empty(); });
  }
  const auto finished = Clock::now();
  const std::clock_t cpu_end = std::clock();
  const double cpu_seconds = cpu_start == static_cast<std::clock_t>(-1) ||
                                     cpu_end == static_cast<std::clock_t>(-1)
                                 ? 0.0
                                 : static_cast<double>(cpu_end - cpu_start) / CLOCKS_PER_SEC;
  const auto elapsed_wall_ms = static_cast<std::uint64_t>(
      std::chrono::duration_cast<std::chrono::milliseconds>(finished - started).count());
  write_artifacts(options, arrivals, started_unix_ms, elapsed_wall_ms, cpu_seconds, *state);
  const bool saturated = state->max_issue_lag_us > options.max_issue_lag_us;
  const bool valid = !saturated && !state->histogram_error &&
                     state->issued == arrivals.size() && state->completed == state->issued &&
                     state->errors == 0;
  return valid ? 0 : 2;
}

}  // namespace

int main(int argc, char** argv) {
  try {
    return run(parse_options(argc, argv));
  } catch (const std::exception& error) {
    std::cerr << "artc_loadgen: " << error.what() << '\n';
    return 1;
  }
}
