# Graph Report - ARTC  (2026-09-30)

## Corpus Check
- Corpus is ~38,003 words - fits in a single context window. You may not need a graph.

## Summary
- 1039 nodes · 1773 edges · 56 communities (54 shown, 2 thin omitted)
- Extraction: 97% EXTRACTED · 3% INFERRED · 0% AMBIGUOUS · INFERRED: 55 edges (avg confidence: 0.84)
- Token cost: 0 input · 0 output

## Community Hubs (Navigation)
- Phase Two Integration Code
- Admission Gate Tests
- Controller Configuration
- Replica State Management
- Workload Arrival Schedules
- Histogram Metrics
- Backend Execution Tests
- Phase Two Controller
- Artifact Analysis Tools
- Router Selection State
- Async Request Lifecycle
- Replica Selector
- Load Generator State
- Command Line Options
- TSan Startup Evidence
- Router Call Lifecycle
- Architecture Documentation
- Example RPC Services
- Adaptive Controller Logic
- Client Request Lifecycle
- Server and Health Services
- Controller Snapshot Data
- Replica Statistics
- Candidate Scoring
- Phase Two Experiment Runner
- Request Context
- Router RPC Handling
- Load Generation
- Deadline Feasibility
- Phase One Gate Runner
- Admission Permit State
- Delay Fault Handling
- Artifact Writer
- Selector Test Coverage
- Probe Service
- Completion State
- Replica Snapshot Metrics
- Selector Interface
- Adaptive Selection Logic
- Selector Implementations
- Replica Window Metrics
- Phase Two Soak Runner
- Async Callback Lifecycle
- Deadline Feasibility Result
- Controller Construction
- Replica Lease Lifecycle
- P2C Selector
- Admission Result Types
- EWMA Selector
- Health Recovery Progress
- Round Robin Selector
- Replica Completion Updates
- Phase Two Gate Runner
- Pinned gRPC Decision
- Controller Worker Lifecycle
- Fault Lab Topology

## God Nodes (most connected - your core abstractions)
1. `ReplicaState` - 66 edges
2. `Phase2Controller` - 55 edges
3. `TEST()` - 42 edges
4. `RunState` - 41 edges
5. `Options` - 38 edges
6. `ACallState` - 35 edges
7. `RouterCallState` - 35 edges
8. `ControllerSnapshot` - 32 edges
9. `ControllerConfig` - 26 edges
10. `RequestContext` - 25 edges

## Surprising Connections (you probably didn't know these)
- `RouterService::Execute()` --calls--> `release`  [INFERRED]
  src/app/services.cc → include/artc/control/phase2.h
- `TEST()` --calls--> `record`  [INFERRED]
  tests/unit/histogram_test.cc → include/artc/bench/histogram.h
- `TEST()` --calls--> `summary`  [INFERRED]
  tests/unit/histogram_test.cc → include/artc/bench/histogram.h
- `TEST()` --calls--> `write_raw`  [INFERRED]
  tests/unit/histogram_test.cc → include/artc/bench/histogram.h
- `RouterService::Execute()` --calls--> `remaining_budget`  [INFERRED]
  src/app/services.cc → include/artc/control/phase2.h

## Import Cycles
- None detected.

## Hyperedges (group relationships)
- **Request Lifecycle Correctness Evidence** — docs_architecture_01_system_architecture_document, docs_architecture_02_request_lifecycle_and_control_document, docs_architecture_03_invariants_and_correctness_document, docs_architecture_05_testing_and_verification_document, docs_architecture_12_traceability_matrix_document [INFERRED 0.85]
- **Standalone gRPC Startup Reproducer Across System, Old Source, and Pinned Stacks** — tests_repro_tsan_grpc_startup_evidence_system_repetitions_system_startup_repetitions, tests_repro_tsan_grpc_startup_evidence_old_source_stack_source_stack_control, tests_repro_tsan_grpc_startup_evidence_pinned_repetitions_startup_repetitions [INFERRED 0.85]

## Communities (56 total, 2 thin omitted)

### Community 0 - "Phase Two Integration Code"
Cohesion: 0.06
Nodes (58): algorithm, barrier, charconv, chrono, cmath, cstddef, cstdint, cstdlib (+50 more)

### Community 1 - "Admission Gate Tests"
Cohesion: 0.05
Nodes (45): AdaptiveSelectorTest, AdmissionGateTest, AimdControllerTest, BoundsSamplesAndLimitsOutlierImpactOnEwma, ConcurrentAdmissionAndReleasePreserveExactAccounting, ConcurrentStartAndStopSerializeWorkerLifecycle, ControllerConfigTest, ControllerPropertyTest (+37 more)

### Community 2 - "Controller Configuration"
Cohesion: 0.05
Nodes (41): AimdConfig, additive_increase, control_interval, initial_limit, max_limit, min_limit, minimum_window_samples, multiplicative_decrease (+33 more)

### Community 3 - "Replica State Management"
Cohesion: 0.05
Nodes (40): array, atomic, size_t, string, ReplicaState, address, completed_, consecutive_failures_ (+32 more)

### Community 4 - "Workload Arrival Schedules"
Cohesion: 0.07
Nodes (38): arrival_schedule, ArrivalScheduleTest, BurstUsesConfiguredPeriodAndDuration, ConstantRateSchedulesIndependentlyOfCompletion, ArrivalScheduleConfig, burst_duration, burst_multiplier, burst_period (+30 more)

### Community 5 - "Histogram Metrics"
Cohesion: 0.06
Nodes (35): EmptySummaryHasNoPercentiles, HdrPercentilesMatchKnownSampleSet, HistogramTest, Deleter, uint64_t, unique_ptr, hdr_histogram, Histogram (+27 more)

### Community 6 - "Backend Execution Tests"
Cohesion: 0.07
Nodes (30): AdaptiveAdmissionRejectsBeforeDispatchAndExplainsAcceptedRoute, DependencyCallsAndAllRoutingPoliciesWork, HealthReportsReadyComponentIdentity, InvalidServiceBRequestReturnsInvalidArgument, RpcServicesTest, ServiceACancellationCompletesAndAllowsSubsequentCalls, address_for(), BlockingBackend (+22 more)

### Community 7 - "Phase Two Controller"
Cohesion: 0.06
Nodes (33): Phase2Controller, admission_counts_, admission_snapshot, backend_attempts_, close_admission, config_, controller_limit_changes_, controller_overload_events_ (+25 more)

### Community 8 - "Artifact Analysis Tools"
Cohesion: 0.13
Nodes (28): argparse, csv, json, a2_reduction_time(), a2_selected_share(), analyze(), direction_changes(), load_run() (+20 more)

### Community 9 - "Router Selection State"
Cohesion: 0.08
Nodes (26): Channel, enable_shared_from_this<RouterService::State>, AdaptiveSelector, config_, explain, score, select, selection_sequence_ (+18 more)

### Community 10 - "Async Request Lifecycle"
Cohesion: 0.08
Nodes (27): ACallState, alarm_, alarm_mutex_, cancelled_, context_, delay_, delay_per_work_unit_us_, dependency_mutex_ (+19 more)

### Community 11 - "Replica Selector"
Cohesion: 0.10
Nodes (24): exception, kLatencyWindowCapacity, array, HealthState, Policy, string, string_view, uint64_t (+16 more)

### Community 12 - "Load Generator State"
Cohesion: 0.07
Nodes (28): pair, condition_variable, mutex, RunState, active, admission_results, attempt_metadata_observed, backend_attempts (+20 more)

### Community 13 - "Command Line Options"
Cohesion: 0.07
Nodes (28): ArrivalMode, path, size_t, uint32_t, Options, allow_errors, compose_project, controller_parameters (+20 more)

### Community 14 - "TSan Startup Evidence"
Cohesion: 0.11
Nodes (27): Generated Probe Protobuf and gRPC Sources, gRPC C++ Library Target, Protobuf Library Target, Standalone gRPC Startup Reproducer Target, ARTC gRPC Lifecycle Audit, ARTC System TSan Binary Linked Libraries, System and Pinned Library TSan Instrumentation Comparison, Old Source Stack Build Recipe (+19 more)

### Community 15 - "Router Call Lifecycle"
Cohesion: 0.10
Nodes (23): BackendRequest, ClientUnaryReactor, size_t, State, SteadyTime, RouterCallState, backend_index_, RouterCallState::BackendReactor (+15 more)

### Community 16 - "Architecture Documentation"
Cohesion: 0.12
Nodes (24): Adaptive Admission Controller, System Architecture Document, Feedback Controller, Bounded Hedging, Request Lifecycle and Control Document, Invariants and Correctness Document, Idempotency and Side-Effect Invariants, Testing and Verification Document (+16 more)

### Community 17 - "Example RPC Services"
Cohesion: 0.09
Nodes (24): shared_ptr, State, string, Stub, uint64_t, v1::Traffic::CallbackService, ReplicaConfig, address (+16 more)

### Community 18 - "Adaptive Controller Logic"
Cohesion: 0.14
Nodes (16): publish, update_health, admission_index(), admission_result_name(), AdmissionResult, atomic, RequestOutcome, size_t (+8 more)

### Community 19 - "Client Request Lifecycle"
Cohesion: 0.12
Nodes (18): multimap, ClientContext, ClientUnaryReactor, milliseconds, shared_ptr, Status, time_point, WorkRequest (+10 more)

### Community 20 - "Server and Health Services"
Cohesion: 0.12
Nodes (17): channel_arg_names, channel_arguments, create_channel, csignal, iomanip, pthread, Server, Service (+9 more)

### Community 21 - "Controller Snapshot Data"
Cohesion: 0.11
Nodes (18): ControllerSnapshot, admission_counts, backend_attempts, controller_limit_changes, controller_overload_events, deadline_goodput, deadline_infeasible, deadline_misses (+10 more)

### Community 22 - "Replica Statistics"
Cohesion: 0.11
Nodes (18): HealthState, time_point, ReplicaStats, completed, consecutive_failures, error_ewma, failed, has_failure (+10 more)

### Community 23 - "Candidate Scoring"
Cohesion: 0.12
Nodes (17): CandidateScore, error_component, health, health_component, inflight, latency_component, load_component, replica_id (+9 more)

### Community 24 - "Phase Two Experiment Runner"
Cohesion: 0.29
Nodes (16): apply_netem(), ARTC_AIMD_TARGET_LATENCY_US, ARTC_GIT_REVISION, ARTC_SERVICE_B_DELAY_US, check_health(), clear_faults(), compose(), COMPOSE_PROJECT_NAME (+8 more)

### Community 25 - "Request Context"
Cohesion: 0.12
Nodes (16): RequestOutcome, SteadyTime, SystemTime, RequestContext, admission_result, arrival_time, caller_deadline, controller_version (+8 more)

### Community 26 - "Router RPC Handling"
Cohesion: 0.32
Nodes (14): CallbackServerContext, HealthRequest, HealthResponse, ServerUnaryReactor, WorkRequest, WorkResponse, start, RouterService::Execute() (+6 more)

### Community 27 - "Load Generation"
Cohesion: 0.20
Nodes (13): ctime, set, string_view, Stub, T, main(), parse_double(), parse_integer() (+5 more)

### Community 28 - "Deadline Feasibility"
Cohesion: 0.22
Nodes (14): remaining_budget, Duration, microseconds, nanoseconds, SteadyTime, SystemTime, evaluate_deadline_feasibility(), make_request_context() (+6 more)

### Community 29 - "Phase One Gate Runner"
Cohesion: 0.32
Nodes (13): ARTC_OUT_DIR, ARTC_ROUTING_POLICY, ARTC_RUN_ID, assert_only_a2_faulted(), capture_qdiscs(), check_health(), cleanup_fault(), compose() (+5 more)

### Community 30 - "Admission Permit State"
Cohesion: 0.15
Nodes (13): AdmissionGate::AdmissionGate(), AdmissionGate::set_limit(), AdmissionPermit::State, accepting, acquired, inflight, limit, max_limit (+5 more)

### Community 31 - "Delay Fault Handling"
Cohesion: 0.18
Nodes (10): Alarm, gpr_timespec, microseconds, mutex, DelayState, alarm_, alarm_mutex_, cancelled_ (+2 more)

### Community 32 - "Artifact Writer"
Cohesion: 0.28
Nodes (13): map, nanoseconds, optional, ostream, string, uint64_t, vector, json_string() (+5 more)

### Community 33 - "Selector Test Coverage"
Cohesion: 0.17
Nodes (12): ConcurrentSelectionsReleaseEveryInflightReservation, EmptyAndNullPoolsFailBeforeSelection, EwmaColdStartSamplesEachReplicaBeforeScoring, LeastInflightUsesStableTieBreakAndExactLeases, P2CSeedIsRepeatableAndSmallPoolsAreDefined, PolicyNamesRejectUnknownValues, RoundRobinSequenceAndCounterWrap, SelectorTest (+4 more)

### Community 34 - "Probe Service"
Cohesion: 0.18
Nodes (10): cstdio, grpcpp, PingRequest, PingResponse, probe, CallbackServerContext, ServerUnaryReactor, main() (+2 more)

### Community 35 - "Completion State"
Cohesion: 0.17
Nodes (7): enable_shared_from_this<CompletionState>, atomic, CompletionState, finished_, reactor_, OwnedServerReactor, state_

### Community 36 - "Replica Snapshot Metrics"
Cohesion: 0.18
Nodes (11): ReplicaSnapshot, completed_total, error_ewma, failed_total, health, id, inflight, latency_ewma_us (+3 more)

### Community 37 - "Selector Interface"
Cohesion: 0.18
Nodes (10): mutex, LeastInflightSelector, choose_locked, name, Selector, choose_locked, name, record_latency (+2 more)

### Community 38 - "Adaptive Selection Logic"
Cohesion: 0.24
Nodes (11): span, AdaptiveSelector::explain(), AdaptiveSelector::score(), AdaptiveSelector::select(), AdmissionPermit::AdmissionPermit(), shared_ptr, uint64_t, vector (+3 more)

### Community 39 - "Selector Implementations"
Cohesion: 0.33
Nodes (10): shared_ptr, size_t, vector, EwmaLatencySelector::choose_locked(), EwmaLatencySelector::record_latency(), LeastInflightSelector::choose_locked(), P2CLatencyInflightSelector::choose_locked(), P2CLatencyInflightSelector::record_latency() (+2 more)

### Community 40 - "Replica Window Metrics"
Cohesion: 0.22
Nodes (9): ReplicaWindow, completed, control_samples, deadline_missed, failed, latency_p95_us, succeeded, timed_out (+1 more)

### Community 41 - "Phase Two Soak Runner"
Cohesion: 0.28
Nodes (8): ARTC_DECISION_SAMPLE_EVERY, ARTC_GIT_REVISION, ARTC_ROUTING_POLICY, ARTC_RUN_ID, compose(), COMPOSE_PROJECT_NAME, on_exit(), run_phase2_soak.sh script

### Community 43 - "Deadline Feasibility Result"
Cohesion: 0.29
Nodes (7): FeasibilityResult, feasible, predicted_queue_delay, predicted_service_latency, remaining, safety_margin, nanoseconds

### Community 44 - "Controller Construction"
Cohesion: 0.29
Nodes (7): make_snapshot, stop, AdaptiveSelector::AdaptiveSelector(), Phase2Controller::close_admission(), Phase2Controller::Phase2Controller(), Phase2Controller::publish(), validate()

### Community 45 - "Replica Lease Lifecycle"
Cohesion: 0.29
Nodes (6): shared_ptr, ReplicaLease, release, replica_, ReplicaLease::ReplicaLease(), Selector::select()

### Community 46 - "P2C Selector"
Cohesion: 0.29
Nodes (7): P2CLatencyInflightSelector, choose_locked, name, random_state_, record_latency, score, smoothing_

### Community 47 - "Admission Result Types"
Cohesion: 0.33
Nodes (6): AcquireResult, permit, result, AdmissionResult, AdmissionGate::try_acquire(), Phase2Controller::try_acquire()

### Community 48 - "EWMA Selector"
Cohesion: 0.33
Nodes (6): EwmaLatencySelector, choose_locked, cold_start_index_, name, record_latency, smoothing_

### Community 49 - "Health Recovery Progress"
Cohesion: 0.40
Nodes (5): uint64_t, HealthProgress, healthy_windows, recovery_successes, slow_windows

### Community 50 - "Round Robin Selector"
Cohesion: 0.40
Nodes (5): uint64_t, RoundRobinSelector, choose_locked, name, next_

### Community 51 - "Replica Completion Updates"
Cohesion: 0.40
Nodes (5): T, time_point, increment_saturated(), ReplicaState::observe_completion(), ReplicaState::record_routed()

### Community 52 - "Phase Two Gate Runner"
Cohesion: 0.83
Nodes (3): build_and_test(), build_and_test_tsan(), run_phase2_gates.sh script

### Community 53 - "Pinned gRPC Decision"
Cohesion: 0.67
Nodes (3): CMake Build Configuration, ADR-001 Pinned gRPC Dependencies, Build gRPC from a Pinned Source Stack

### Community 54 - "Controller Worker Lifecycle"
Cohesion: 0.67
Nodes (3): tick, Phase2Controller::run(), stop_token

## Knowledge Gaps
- **386 isolated node(s):** `mode`, `duration`, `rate_rps`, `initial_rate_rps`, `burst_multiplier` (+381 more)
  These have ≤1 connection - possible missing edges or undocumented components. (Counts symbols only; 565 node(s) total have ≤1 connection when file, concept and rationale nodes are included.)
- **2 thin communities (<3 nodes) omitted from report** — run `graphify query` to explore isolated nodes.

## Suggested Questions
_Questions this graph is uniquely positioned to answer:_

- **Why does `Phase2Controller` connect `Phase Two Controller` to `Phase Two Integration Code`, `Admission Gate Tests`, `Controller Configuration`, `Replica State Management`, `Router Selection State`, `Controller Construction`, `Health Recovery Progress`, `Adaptive Controller Logic`, `Controller Snapshot Data`, `Controller Worker Lifecycle`, `Candidate Scoring`, `Request Context`?**
  _High betweenness centrality (0.106) - this node is a cross-community bridge._
- **Why does `ReplicaState` connect `Replica State Management` to `Phase Two Integration Code`, `Admission Gate Tests`, `Selector Test Coverage`, `Selector Interface`, `Adaptive Selection Logic`, `Phase Two Controller`, `Selector Implementations`, `Router Selection State`, `Async Request Lifecycle`, `Replica Selector`, `Controller Construction`, `Replica Lease Lifecycle`, `Round Robin Selector`, `Replica Statistics`?**
  _High betweenness centrality (0.098) - this node is a cross-community bridge._
- **Why does `RunState` connect `Load Generator State` to `Client Request Lifecycle`, `Artifact Writer`, `Load Generation`, `Histogram Metrics`?**
  _High betweenness centrality (0.059) - this node is a cross-community bridge._
- **Are the 11 inferred relationships involving `TEST()` (e.g. with `close` and `set_limit`) actually correct?**
  _`TEST()` has 11 INFERRED edges - model-reasoned connections that need verification._
- **What connects `mode`, `duration`, `rate_rps` to the rest of the system?**
  _386 weakly-connected nodes found - possible documentation gaps or missing edges._
- **Should `Phase Two Integration Code` be split into smaller, more focused modules?**
  _Cohesion score 0.06278538812785388 - nodes in this community are weakly interconnected._
- **Should `Admission Gate Tests` be split into smaller, more focused modules?**
  _Cohesion score 0.04734299516908213 - nodes in this community are weakly interconnected._