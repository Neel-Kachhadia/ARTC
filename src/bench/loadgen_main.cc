#include "artc/bench/arrival_schedule.h"
#include "artc/bench/histogram.h"
#include "artc/rpc/services.h"

#include <algorithm>
#include <array>
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
#include <optional>
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
  std::map<std::string, std::string> controller_parameters;
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
  bool allow_errors{false};
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
  constexpr std::array parameters{
      std::pair{"ARTC_SERVICE_B_DELAY_US", "0"},
      std::pair{"ARTC_AIMD_MIN_LIMIT", "1"},
      std::pair{"ARTC_AIMD_MAX_LIMIT", "512"},
      std::pair{"ARTC_AIMD_INITIAL_LIMIT", "64"},
      std::pair{"ARTC_AIMD_ALPHA", "2"},
      std::pair{"ARTC_AIMD_BETA", "0.7"},
      std::pair{"ARTC_AIMD_INTERVAL_MS", "100"},
      std::pair{"ARTC_AIMD_MIN_SAMPLES", "16"},
      std::pair{"ARTC_AIMD_TARGET_LATENCY_US", "50000"},
      std::pair{"ARTC_AIMD_OVERLOAD_ERROR_FRACTION", "0.1"},
      std::pair{"ARTC_DEADLINE_MARGIN_US", "1000"},
      std::pair{"ARTC_DEFAULT_DEADLINE_MS", "5000"},
      std::pair{"ARTC_DECISION_SAMPLE_EVERY", "0"},
      std::pair{"ARTC_HEALTH_MIN_SAMPLES", "4"},
      std::pair{"ARTC_HEALTH_FAILURES", "3"},
      std::pair{"ARTC_HEALTH_RECOVERY_SUCCESSES", "3"},
      std::pair{"ARTC_HEALTH_RECOVERY_COOLDOWN_MS", "500"},
      std::pair{"ARTC_HEALTH_DEGRADED_RATIO", "2.0"},
      std::pair{"ARTC_HEALTH_RECOVERED_RATIO", "1.5"},
      std::pair{"ARTC_RECOVERY_PROBE_PERIOD", "16"},
      std::pair{"ARTC_ATTEMPT_MAX_TOTAL", "3"},
      std::pair{"ARTC_ATTEMPT_MAX_ACTIVE", "2"},
      std::pair{"ARTC_ATTEMPT_JITTER_SEED", "1"},
      std::pair{"ARTC_ATTEMPT_MINIMUM_BUDGET_US", "1000"},
      std::pair{"ARTC_HEDGE_BUDGET_CAPACITY", "10"},
      std::pair{"ARTC_HEDGE_BUDGET_REFILL_PER_SECOND", "1.0"},
      std::pair{"ARTC_RETRY_BUDGET_CAPACITY", "10"},
      std::pair{"ARTC_RETRY_BUDGET_REFILL_PER_SECOND", "1.0"},
      std::pair{"ARTC_EXECUTE_IDEMPOTENCY", "non_idempotent"},
      std::pair{"ARTC_EXECUTE_HEDGING_ENABLED", "false"},
      std::pair{"ARTC_EXECUTE_RETRY_ENABLED", "false"},
      std::pair{"ARTC_EXECUTE_ALLOW_SAME_REPLICA_RETRY", "false"},
      std::pair{"ARTC_EXECUTE_MAX_TOTAL_ATTEMPTS", "1"},
      std::pair{"ARTC_EXECUTE_MAX_RETRIES", "0"},
      std::pair{"ARTC_EXECUTE_HEDGE_DELAY_MIN_US", "10000"},
      std::pair{"ARTC_EXECUTE_HEDGE_DELAY_MAX_US", "100000"},
      std::pair{"ARTC_EXECUTE_RETRY_BACKOFF_BASE_MS", "10"},
      std::pair{"ARTC_EXECUTE_RETRY_BACKOFF_MAX_MS", "100"},
      std::pair{"ARTC_EXECUTE_RETRY_JITTER_MAX_MS", "10"},
      std::pair{"ARTC_EXECUTE_RETRYABLE_STATUSES", "UNAVAILABLE"},
      std::pair{"ARTC_SERVICE_A_UNAVAILABLE_FIRST_N", "0"},
      std::pair{"ARTC_SERVICE_A_HONOR_CANCELLATION", "true"},
      std::pair{"ARTC_SERVICE_A1_DELAY_US", "0"},
      std::pair{"ARTC_SERVICE_A2_DELAY_US", "0"},
      std::pair{"ARTC_SERVICE_A3_DELAY_US", "0"},
      std::pair{"ARTC_SERVICE_A1_UNAVAILABLE_FIRST_N", "0"},
      std::pair{"ARTC_SERVICE_A2_UNAVAILABLE_FIRST_N", "0"},
      std::pair{"ARTC_SERVICE_A3_UNAVAILABLE_FIRST_N", "0"},
      std::pair{"ARTC_SERVICE_A1_HONOR_CANCELLATION", "true"},
      std::pair{"ARTC_SERVICE_A2_HONOR_CANCELLATION", "true"},
      std::pair{"ARTC_SERVICE_A3_HONOR_CANCELLATION", "true"}};
  for (const auto& [name, fallback] : parameters) {
    const char* value = std::getenv(name);
    options.controller_parameters.emplace(name, value == nullptr ? fallback : value);
  }
  for (int index = 1; index < argc; ++index) {
    const std::string_view key(argv[index]);
    if (key == "--help") {
      std::cout << "artc_loadgen --target HOST:PORT --output DIR [--mode constant|poisson|step|ramp|burst|scripted] "
                   "[--rate-rps N] [--initial-rate-rps N] [--duration-ms N] [--seed N] "
                   "[--max-inflight N] [--max-issue-lag-us N] [--deadline-ms N] "
                   "[--work-units N] [--payload-bytes N] [--invoke-dependency] [--allow-errors]\n"
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
    if (key == "--allow-errors") {
      options.allow_errors = true;
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

void write_counts(std::ostream& output, const std::map<std::string, std::uint64_t>& counts) {
  output << '{';
  bool first = true;
  for (const auto& [name, count] : counts) {
    if (!first) output << ',';
    first = false;
    output << json_string(name) << ':' << count;
  }
  output << '}';
}

void write_strings(std::ostream& output, const std::map<std::string, std::string>& values) {
  output << '{';
  bool first = true;
  for (const auto& [name, value] : values) {
    if (!first) output << ',';
    first = false;
    output << json_string(name) << ':' << json_string(value);
  }
  output << '}';
}

void write_csv_field(std::ostream& output, std::string_view value) {
  output << '"';
  for (const char character : value) {
    if (character == '"') output << '"';
    output << character;
  }
  output << '"';
}

class ClientCall;

struct RunState {
  explicit RunState(std::chrono::milliseconds call_deadline,
                    Clock::time_point run_start)
      : deadline(call_deadline), started(run_start) {}

  void complete(ClientCall* call, const grpc::Status& status,
                const artc::v1::WorkResponse& response, Clock::time_point scheduled,
                Clock::time_point issued_at,
                const std::multimap<grpc::string_ref, grpc::string_ref>& metadata);

  const std::chrono::milliseconds deadline;
  const Clock::time_point started;
  // ponytail: one per-run lock keeps callback accounting bounded; shard only if issue lag shows contention.
  std::mutex mutex;
  std::condition_variable changed;
  std::set<ClientCall*> active;
  artc::bench::Histogram latency;
  artc::bench::Histogram issue_lag;
  std::map<std::string, std::uint64_t> by_replica;
  std::map<std::string, std::uint64_t> admission_results;
  std::map<std::string, std::uint64_t> statuses;
  std::map<std::string, std::uint64_t> winning_attempt_kinds;
  std::vector<std::pair<std::uint64_t, std::string>> decision_samples;
  std::uint64_t issued{0};
  std::uint64_t completed{0};
  std::uint64_t succeeded{0};
  std::uint64_t deadline_goodput{0};
  std::uint64_t errors{0};
  std::uint64_t rejected{0};
  std::uint64_t deadline_misses{0};
  std::uint64_t backend_attempts{0};
  std::uint64_t attempt_metadata_observed{0};
  std::uint64_t phase3_attempt_metadata_observed{0};
  std::uint64_t phase3_admitted_metadata_observed{0};
  std::uint64_t primary_attempts{0};
  std::uint64_t hedge_attempts{0};
  std::uint64_t retry_attempts{0};
  std::uint64_t cancelled_attempts{0};
  std::uint64_t admitted_without_attempts{0};
  std::uint64_t min_admitted_attempts_per_request{UINT64_MAX};
  std::uint64_t max_admitted_attempts_per_request{0};
  std::uint64_t invariant_violations{0};
  std::uint64_t decision_samples_dropped{0};
  std::uint64_t max_issue_lag_us{0};
  bool histogram_error{false};
};

class ClientCall final : public grpc::ClientUnaryReactor {
 public:
  ClientCall(std::shared_ptr<RunState> state, Clock::time_point scheduled,
             const Options& options, std::uint64_t request_id)
      : state_(std::move(state)), scheduled_(scheduled), issued_at_(Clock::now()) {
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
    state_->complete(this, status, response_, scheduled_, issued_at_,
                     context_.GetServerTrailingMetadata());
    delete this;
  }

 private:
  friend struct RunState;
  std::shared_ptr<RunState> state_;
  Clock::time_point scheduled_;
  Clock::time_point issued_at_;
  grpc::ClientContext context_;
  artc::v1::WorkRequest request_;
  artc::v1::WorkResponse response_;
};

void RunState::complete(ClientCall* call, const grpc::Status& status,
                        const artc::v1::WorkResponse& response,
                        Clock::time_point scheduled, Clock::time_point issued_at,
                        const std::multimap<grpc::string_ref, grpc::string_ref>& metadata) {
  const auto finished = Clock::now();
  const auto elapsed = finished - scheduled;
  const auto rpc_elapsed = finished - issued_at;
  const auto elapsed_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(elapsed);
  std::lock_guard lock(mutex);
  if (!latency.record(elapsed_ns)) histogram_error = true;
  ++completed;
  ++statuses[std::to_string(static_cast<int>(status.error_code()))];
  std::string admission;
  std::string selected_replica;
  std::string decision;
  std::optional<std::uint64_t> attempts;
  std::optional<std::uint64_t> primary_attempt_count;
  std::optional<std::uint64_t> hedge_attempt_count;
  std::optional<std::uint64_t> retry_attempt_count;
  std::optional<std::uint64_t> cancelled_attempt_count;
  std::optional<std::string> winning_attempt_kind;
  const auto parse_metadata_count = [this](std::string_view value,
                                           std::optional<std::uint64_t>* destination) {
    if (*destination) {
      ++invariant_violations;
      return;
    }
    std::uint64_t parsed = 0;
    const auto [end, error] = std::from_chars(value.data(), value.data() + value.size(), parsed);
    if (error == std::errc{} && end == value.data() + value.size()) {
      *destination = parsed;
    } else {
      ++invariant_violations;
    }
  };
  for (const auto& [key_ref, value_ref] : metadata) {
    const std::string_view key(key_ref.data(), key_ref.size());
    const std::string value(value_ref.data(), value_ref.size());
    if (key == "artc-admission-result") {
      if (!admission.empty()) ++invariant_violations;
      admission = value;
    } else if (key == "artc-backend-attempts") {
      parse_metadata_count(value, &attempts);
    } else if (key == "artc-primary-attempts") {
      parse_metadata_count(value, &primary_attempt_count);
    } else if (key == "artc-hedge-attempts") {
      parse_metadata_count(value, &hedge_attempt_count);
    } else if (key == "artc-retry-attempts") {
      parse_metadata_count(value, &retry_attempt_count);
    } else if (key == "artc-cancelled-attempts") {
      parse_metadata_count(value, &cancelled_attempt_count);
    } else if (key == "artc-winning-attempt-kind") {
      if (winning_attempt_kind) ++invariant_violations;
      else winning_attempt_kind = value;
    } else if (key == "artc-selected-replica") {
      selected_replica = value;
    } else if (key == "artc-decision") {
      decision = value;
    }
  }
  const bool any_phase3_counts = primary_attempt_count || hedge_attempt_count ||
                                 retry_attempt_count || cancelled_attempt_count;
  const bool complete_phase3_counts = primary_attempt_count && hedge_attempt_count &&
                                      retry_attempt_count && cancelled_attempt_count;
  if (any_phase3_counts && !complete_phase3_counts) ++invariant_violations;
  if (complete_phase3_counts) {
    ++phase3_attempt_metadata_observed;
    const bool counts_bounded = *primary_attempt_count <= 3 &&
                                *hedge_attempt_count <= 3 &&
                                *retry_attempt_count <= 3;
    if (!counts_bounded) ++invariant_violations;
    const std::uint64_t classified_attempts = counts_bounded
        ? *primary_attempt_count + *hedge_attempt_count + *retry_attempt_count
        : 0;
    if (counts_bounded && (!attempts || classified_attempts != *attempts ||
                           classified_attempts > 3)) {
      ++invariant_violations;
    }
    primary_attempts += *primary_attempt_count;
    hedge_attempts += *hedge_attempt_count;
    retry_attempts += *retry_attempt_count;
    if (!attempts || *cancelled_attempt_count > *attempts) ++invariant_violations;
    cancelled_attempts += *cancelled_attempt_count;
  }
  if (winning_attempt_kind) {
    if (*winning_attempt_kind != "primary" && *winning_attempt_kind != "hedge" &&
        *winning_attempt_kind != "retry") {
      ++invariant_violations;
    } else {
      ++winning_attempt_kinds[*winning_attempt_kind];
    }
  }
  if (!admission.empty()) {
    ++admission_results[admission];
    if (admission == "ADMITTED") {
      if (complete_phase3_counts) {
        const bool no_dispatch_terminal =
            status.error_code() == grpc::StatusCode::CANCELLED ||
            status.error_code() == grpc::StatusCode::DEADLINE_EXCEEDED;
        const bool attempts_valid = attempts && *attempts <= 3 &&
            ((*attempts == 0 && no_dispatch_terminal &&
              *primary_attempt_count == 0 && *hedge_attempt_count == 0 &&
              *retry_attempt_count == 0) ||
             (*attempts > 0 && *primary_attempt_count == 1));
        if (!attempts_valid) {
          ++invariant_violations;
        } else {
          ++phase3_admitted_metadata_observed;
          if (*attempts == 0) ++admitted_without_attempts;
          min_admitted_attempts_per_request =
              std::min(min_admitted_attempts_per_request, *attempts);
          max_admitted_attempts_per_request =
              std::max(max_admitted_attempts_per_request, *attempts);
        }
        if (status.ok() && !winning_attempt_kind) ++invariant_violations;
        if (winning_attempt_kind &&
            ((*winning_attempt_kind == "primary" && *primary_attempt_count == 0) ||
             (*winning_attempt_kind == "hedge" && *hedge_attempt_count == 0) ||
             (*winning_attempt_kind == "retry" && *retry_attempt_count == 0))) {
          ++invariant_violations;
        }
      } else if (attempts && *attempts > 1) {
        ++invariant_violations;
      }
    } else if (admission.starts_with("REJECT_")) {
      ++rejected;
      if (attempts && *attempts != 0) ++invariant_violations;
      if (complete_phase3_counts &&
          (*primary_attempt_count != 0 || *hedge_attempt_count != 0 ||
           *retry_attempt_count != 0)) {
        ++invariant_violations;
      }
    }
    if (!attempts) ++invariant_violations;
  }
  if (attempts) {
    ++attempt_metadata_observed;
    backend_attempts += *attempts;
    if (*attempts != 0 && !selected_replica.empty()) ++by_replica[selected_replica];
  } else if (status.ok()) {
    backend_attempts += response.backend_attempt_count();
    if (!response.replica_id().empty()) ++by_replica[response.replica_id()];
  }
  if (!decision.empty()) {
    constexpr std::size_t kMaximumDecisionSamples = 10'000;
    if (decision_samples.size() < kMaximumDecisionSamples) {
      const auto elapsed_us = static_cast<std::uint64_t>(
          std::chrono::duration_cast<std::chrono::microseconds>(issued_at - started).count());
      decision_samples.emplace_back(elapsed_us, std::move(decision));
    } else {
      ++decision_samples_dropped;
    }
  }
  const bool rejected_before_dispatch = admission.starts_with("REJECT_");
  if (rpc_elapsed > deadline ||
      (status.error_code() == grpc::StatusCode::DEADLINE_EXCEEDED &&
       !rejected_before_dispatch)) {
    ++deadline_misses;
  }
  if (status.ok()) {
    ++succeeded;
    if (rpc_elapsed <= deadline) {
      ++deadline_goodput;
    }
  } else {
    ++errors;
  }
  active.erase(call);
  changed.notify_all();
}

std::uint64_t admitted_request_count(const RunState& state) {
  const auto admitted = state.admission_results.find("ADMITTED");
  return admitted == state.admission_results.end() ? 0 : admitted->second;
}

bool attempt_accounting_valid(const RunState& state) {
  if (state.phase3_attempt_metadata_observed == 0) {
    // Preserve Phase 2's single-primary invariant for legacy trailers.
    return state.backend_attempts <= state.issued;
  }
  if (state.phase3_attempt_metadata_observed != admitted_request_count(state) ||
      state.attempt_metadata_observed != state.issued ||
      state.phase3_admitted_metadata_observed != admitted_request_count(state)) {
    return false;
  }
  const auto admitted = admitted_request_count(state);
  if (admitted > UINT64_MAX / 3) return false;
  const auto attempt_limit = 3 * admitted;
  if (state.backend_attempts > attempt_limit ||
      state.primary_attempts > state.backend_attempts) {
    return false;
  }
  const auto after_primary = state.backend_attempts - state.primary_attempts;
  if (state.hedge_attempts > after_primary) return false;
  return state.retry_attempts == after_primary - state.hedge_attempts;
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
  {
    std::ofstream samples(options.output / "decision-samples.csv", std::ios::out | std::ios::trunc);
    if (!samples) throw std::runtime_error("cannot create decision sample artifact");
    samples << "elapsed_us,decision\n";
    for (const auto& [elapsed_us, decision] : state.decision_samples) {
      samples << elapsed_us << ',';
      write_csv_field(samples, decision);
      samples << '\n';
    }
    samples.flush();
    if (!samples) throw std::runtime_error("failed to write decision sample artifact");
  }

  const auto latency = state.latency.summary();
  const auto issue_lag = state.issue_lag.summary();
  const bool saturated = state.max_issue_lag_us > options.max_issue_lag_us;
  const bool measurement_valid = !saturated && !state.histogram_error &&
                                 state.invariant_violations == 0 &&
                                 state.issued == arrivals.size() &&
                                 state.completed == state.issued &&
                                 attempt_accounting_valid(state);
  const bool valid = measurement_valid && (options.allow_errors || state.errors == 0);
  const double offered_rps = static_cast<double>(arrivals.size()) /
                             std::chrono::duration<double>(options.duration).count();
  const double duration_seconds = std::chrono::duration<double>(options.duration).count();
  const auto count_rps = [duration_seconds](std::uint64_t count) {
    return static_cast<double>(count) / duration_seconds;
  };
  const auto admitted_count = admitted_request_count(state);
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
           << ",\n\"admitted\":" << admitted_count
           << ",\n\"rejected\":" << state.rejected
           << ",\n\"admitted_rps\":" << count_rps(admitted_count)
           << ",\n\"rejected_rps\":" << count_rps(state.rejected)
           << ",\n\"completed_rps\":" << count_rps(state.completed)
           << ",\n\"deadline_goodput\":" << state.deadline_goodput
           << ",\n\"deadline_goodput_rps\":" << count_rps(state.deadline_goodput)
           << ",\n\"deadline_misses\":" << state.deadline_misses
           << ",\n\"errors\":" << state.errors
           << ",\n\"backend_attempts\":" << state.backend_attempts
           << ",\n\"attempt_metadata_observed\":" << state.attempt_metadata_observed
           << ",\n\"attempt_amplification\":" << amplification;
  if (state.phase3_attempt_metadata_observed != 0) {
    manifest << ",\n\"attempt_metadata_version\":3"
             << ",\n\"phase3_attempt_metadata_observed\":"
             << state.phase3_attempt_metadata_observed
             << ",\n\"phase3_admitted_metadata_observed\":"
             << state.phase3_admitted_metadata_observed
             << ",\n\"admitted_without_attempts\":"
             << state.admitted_without_attempts
             << ",\n\"primary_attempts\":" << state.primary_attempts
             << ",\n\"hedge_attempts\":" << state.hedge_attempts
             << ",\n\"retry_attempts\":" << state.retry_attempts
             << ",\n\"cancelled_attempts\":" << state.cancelled_attempts
             << ",\n\"min_admitted_attempts_per_request\":";
    if (admitted_count == 0) manifest << "null";
    else manifest << state.min_admitted_attempts_per_request;
    manifest
             << ",\n\"max_admitted_attempts_per_request\":"
             << state.max_admitted_attempts_per_request
             << ",\n\"attempt_amplification_per_admitted\":";
    if (admitted_count == 0) {
      manifest << "null";
    } else {
      manifest << static_cast<double>(state.backend_attempts) /
                    static_cast<double>(admitted_count);
    }
    manifest << ",\n\"winning_attempt_kinds\":";
    write_counts(manifest, state.winning_attempt_kinds);
  }
  manifest << ",\n\"invariant_violations\":" << state.invariant_violations
           << ",\n\"decision_samples\":" << state.decision_samples.size()
           << ",\n\"decision_samples_dropped\":" << state.decision_samples_dropped
           << ",\n\"max_issue_lag_us\":" << state.max_issue_lag_us
           << ",\n\"max_issue_lag_limit_us\":" << options.max_issue_lag_us
           << ",\n\"generator_saturated\":" << (saturated ? "true" : "false")
           << ",\n\"measurement_valid\":" << (measurement_valid ? "true" : "false")
           << ",\n\"allow_errors\":" << (options.allow_errors ? "true" : "false")
           << ",\n\"valid\":" << (valid ? "true" : "false")
           << ",\n\"process_cpu_seconds\":" << process_cpu_seconds
           << ",\n\"process_cpu_percent\":" << cpu_percent
           << ",\n\"cpu_seconds_per_completed_request\":" << cpu_seconds_per_request
           << ",\n\"latency_us\":";
  write_summary(manifest, latency);
  manifest << ",\n\"issue_lag_us\":";
  write_summary(manifest, issue_lag);
  manifest << ",\n\"admission_results\":";
  write_counts(manifest, state.admission_results);
  manifest << ",\n\"status_code_counts\":";
  write_counts(manifest, state.statuses);
  manifest << ",\n\"controller_parameters\":";
  write_strings(manifest, options.controller_parameters);
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
  const auto started = Clock::now();
  auto state = std::make_shared<RunState>(options.deadline, started);
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
                     state->invariant_violations == 0 &&
                     state->issued == arrivals.size() && state->completed == state->issued &&
                     attempt_accounting_valid(*state) &&
                     (options.allow_errors || state->errors == 0);
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
