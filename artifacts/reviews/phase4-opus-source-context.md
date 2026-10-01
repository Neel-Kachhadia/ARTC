# Focused Phase 4 early-review source context

## src/app/services.cc

### Lines 45-75
```cpp
45: enum class TimerSlot : std::uint8_t { kHedge, kRetry, kDeadline };
46: 
47: class BackendCallbackDrain {
48:  public:
49:   void register_callback() {
50:     std::lock_guard lock(mutex_);
51:     ++pending_;
52:   }
53: 
54:   void callback_destroyed() noexcept {
55:     std::lock_guard lock(mutex_);
56:     if (pending_ == 0) std::terminate();
57:     --pending_;
58:     if (pending_ == 0) cv_.notify_all();
59:   }
60: 
61:   [[nodiscard]] std::uint64_t pending() const noexcept {
62:     std::lock_guard lock(mutex_);
63:     return pending_;
64:   }
65: 
66:   [[nodiscard]] bool wait_until(std::chrono::steady_clock::time_point deadline) const {
67:     std::unique_lock lock(mutex_);
68:     return cv_.wait_until(lock, deadline, [&] { return pending_ == 0; });
69:   }
70: 
71:  private:
72:   mutable std::mutex mutex_;
73:   mutable std::condition_variable cv_;
74:   std::uint64_t pending_{0};
75: };
```

### Lines 710-920
```cpp
710:   }
711:   return result;
712: }
713: 
714: class AttemptManager final : public CompletionState {
715:  public:
716:   struct BackendAttempt;
717: 
718:   AttemptManager(grpc::ServerUnaryReactor* reactor,
719:                  grpc::CallbackServerContext* context,
720:                  artc::v1::WorkResponse* response,
721:                  std::shared_ptr<RouterService::State> owner,
722:                  LogicalRequest logical, routing::ReplicaLease primary_lease,
723:                  control::AdmissionPermit permit, std::size_t backend_index,
724:                  std::string decision_metadata)
725:       : CompletionState(reactor),
726:         context_(context),
727:         response_(response),
728:         owner_(std::move(owner)),
729:         logical_(std::move(logical)),
730:         primary_lease_(std::move(primary_lease)),
731:         permit_(std::move(permit)),
732:         primary_backend_index_(backend_index),
733:         decision_metadata_(std::move(decision_metadata)),
734: #if defined(ARTC_ENABLE_TEST_HOOKS)
735:         test_manager_id_(owner_->next_test_manager_id.fetch_add(
736:             1, std::memory_order_relaxed)),
737: #endif
738:         total_attempt_limit_(std::min(logical_.policy.max_total_attempts,
739:                                       owner_->attempt_config.max_total_attempts)) {
740:     if (const auto replica = primary_lease_.replica()) primary_replica_id_ = replica->id;
741:     saturating_add(owner_->metrics.logical_requests, 1);
742:   }
743: 
744:   struct BackendAttempt final : DependencyRequest {
745:     explicit BackendAttempt(routing::ReplicaLease attempt_lease)
746:         : lease(std::move(attempt_lease)) {}
747: 
748:     std::uint64_t id{0};
749:     AttemptKind kind{AttemptKind::kPrimary};
750:     AttemptState state{AttemptState::kCreated};
751:     std::size_t backend_index{0};
752:     std::string replica_id;
753:     control::SteadyTime started_at{};
754:     control::SteadyTime deadline{};
755:     control::SteadyTime cancellation_requested_at{};
756:     routing::ReplicaLease lease;
757:     grpc::Status status;
758:     bool cancel_requested{false};
759:     AttemptCancellationReason cancellation_reason{AttemptCancellationReason::kInternal};
760:     bool censored_by_winner{false};
761:     bool accounted{false};
762:   };
763: 
764:   void start() noexcept {
765:     try {
766:       const auto self = std::static_pointer_cast<AttemptManager>(shared_from_this());
767:       const std::weak_ptr<AttemptManager> weak_self = self;
768:       shutdown_callback_.emplace(
769:           owner_->shutdown_source.get_token(),
770:           std::function<void()>([weak_self] {
771:             if (const auto manager = weak_self.lock()) manager->shutdown();
772:           }));
773:       CompletionPlan plan;
774:       {
775:         std::lock_guard lock(mutex_);
776:         if (state_ != LogicalState::kActive) return;
777:         if (owner_->dispatch_closed.load(std::memory_order_acquire) ||
778:             owner_->shutdown_source.stop_requested()) {
779:           finish_locked({grpc::StatusCode::CANCELLED, "router is shutting down"},
780:                         control::RequestOutcome::kCancelled, nullptr, &plan,
781:                         AttemptCancellationReason::kShutdown);
782:         } else if (context_->IsCancelled()) {
783:           finish_cancelled_locked(&plan);
784:         } else {
785:           schedule_deadline_locked(weak_self);
786:           const auto result = start_attempt_locked(
787:               AttemptKind::kPrimary, primary_backend_index_, std::move(primary_lease_),
788:               std::chrono::nanoseconds::zero(), static_cast<NoBudget*>(nullptr), &plan);
789:           static_cast<void>(result);
790:         }
791:       }
792:       execute_plan(std::move(plan));
793:     } catch (const std::exception& error) {
794:       fail_internal(error.what());
795:     } catch (...) {
796:       fail_internal("unexpected attempt setup failure");
797:     }
798:   }
799: 
800:   void cancel() override {
801:     CompletionPlan plan;
802:     {
803:       std::lock_guard lock(mutex_);
804:       if (state_ != LogicalState::kActive) return;
805:       const bool deadline_expired = logical_deadline_expired_locked();
806:       if (deadline_expired) {
807:         finish_locked({grpc::StatusCode::DEADLINE_EXCEEDED,
808:                        "logical request deadline expired"},
809:                       control::RequestOutcome::kDeadlineMiss, nullptr, &plan,
810:                       AttemptCancellationReason::kDeadline);
811:       } else {
812:         finish_locked({grpc::StatusCode::CANCELLED, "router request cancelled"},
813:                       control::RequestOutcome::kCancelled, nullptr, &plan,
814:                       AttemptCancellationReason::kCaller);
815:       }
816:     }
817:     execute_plan(std::move(plan));
818:   }
819: 
820:   void shutdown() noexcept {
821:     CompletionPlan plan;
822:     {
823:       std::lock_guard lock(mutex_);
824:       if (state_ != LogicalState::kActive) return;
825:       finish_locked({grpc::StatusCode::CANCELLED, "router is shutting down"},
826:                     control::RequestOutcome::kCancelled, nullptr, &plan,
827:                     AttemptCancellationReason::kShutdown);
828:     }
829:     execute_plan(std::move(plan));
830:   }
831: 
832:   void backend_done(const std::shared_ptr<BackendAttempt>& attempt,
833:                     const grpc::Status& status) noexcept {
834:     const auto now = owner_->now();
835:     CompletionPlan plan;
836:     control::RequestOutcome attempt_outcome = control::RequestOutcome::kFailure;
837:     bool was_terminal = false;
838:     bool record_censored = false;
839:     double censored_lower_bound_us = 0.0;
840:     {
841:       std::lock_guard lock(mutex_);
842:       if (attempt->accounted) return;
843:       if (active_attempts_ == 0) std::terminate();
844:       was_terminal = state_ == LogicalState::kCompleted;
845:       const auto decision_now = owner_->now();
846:       const bool deadline_expired =
847:           !was_terminal && logical_deadline_expired_at(decision_now);
848:       attempt->accounted = true;
849:       attempt->status = status;
850:       --active_attempts_;
851:       owner_->metrics.active_attempts.fetch_sub(1, std::memory_order_relaxed);
852:       saturating_add(owner_->metrics.completions[attempt_kind_index(attempt->kind)], 1);
853: 
854:       if (status.ok()) {
855:         attempt->state = AttemptState::kSucceeded;
856:         const bool request_timed_out =
857:             (!was_terminal && deadline_expired) ||
858:             (was_terminal && logical_.context.terminal_outcome ==
859:                                  control::RequestOutcome::kDeadlineMiss);
860:         attempt_outcome = request_timed_out
861:                               ? control::RequestOutcome::kDeadlineMiss
862:                               : control::RequestOutcome::kSuccess;
863:       } else if (status.error_code() == grpc::StatusCode::DEADLINE_EXCEEDED) {
864:         attempt->state = AttemptState::kTimedOut;
865:         attempt_outcome = control::RequestOutcome::kTimeout;
866:       } else if (status.error_code() == grpc::StatusCode::CANCELLED &&
867:                  (attempt->cancel_requested || (!was_terminal && deadline_expired))) {
868:         attempt->state = AttemptState::kCancelled;
869:         if (!attempt->cancel_requested && deadline_expired) {
870:           attempt->cancellation_reason = AttemptCancellationReason::kDeadline;
871:           attempt->cancellation_requested_at = decision_now;
872:         }
873:         attempt_outcome = attempt->cancellation_reason ==
874:                                   AttemptCancellationReason::kDeadline
875:                               ? control::RequestOutcome::kTimeout
876:                               : control::RequestOutcome::kCancelled;
877:         record_censored = attempt->censored_by_winner;
878:         if (record_censored) {
879:           censored_lower_bound_us = attempt_latency_us(
880:               attempt->cancellation_requested_at, attempt->started_at);
881:         }
882:       } else {
883:         attempt->state = AttemptState::kFailed;
884:         attempt_outcome = control::RequestOutcome::kFailure;
885:       }
886: 
887:       if (!was_terminal) {
888:         if (deadline_expired) {
889:           finish_locked({grpc::StatusCode::DEADLINE_EXCEEDED,
890:                          "logical request deadline expired"},
891:                         control::RequestOutcome::kDeadlineMiss, nullptr, &plan,
892:                         AttemptCancellationReason::kDeadline);
893:         } else if (status.ok()) {
894:           finish_locked(grpc::Status::OK, control::RequestOutcome::kSuccess,
895:                         attempt.get(), &plan);
896:         } else {
897:           if (attempt->kind == AttemptKind::kPrimary) cancel_hedge_locked();
898:           advance_after_failure_locked(&plan);
899:         }
900:       } else {
901:         record_wasted_time_locked(*attempt, now);
902:       }
903:       release_permit_if_drained_locked();
904:     }
905: 
906:     if (owner_->controller) {
907:       const auto observed_at =
908:           status.error_code() == grpc::StatusCode::CANCELLED &&
909:                   attempt->cancellation_reason == AttemptCancellationReason::kDeadline
910:               ? attempt->cancellation_requested_at
911:               : now;
912:       const double latency_us = attempt_latency_us(observed_at, attempt->started_at);
913:       static_cast<void>(owner_->controller->record_attempt_completion(
914:           attempt->backend_index, observed_at, latency_us, attempt_outcome));
915:     } else if (status.ok()) {
916:       owner_->selector->record_latency(attempt->lease.replica(), attempt_latency_us(
917:           now, attempt->started_at));
918:     }
919:     if (record_censored && attempt->lease.replica()->record_censored_latency_lower_bound(
920:                                censored_lower_bound_us,
```

### Lines 945-980
```cpp
945:       auto drain = std::move(drain_);
946:       attempt_.reset();
947:       manager_.reset();
948:       if (drain) drain->callback_destroyed();
949:     }
950: 
951:     void register_for_drain() {
952:       drain_ = manager_->register_backend_reactor();
953:     }
954: 
955:     void OnDone(const grpc::Status& status) override {
956:       manager_->backend_done(attempt_, status);
957:       delete this;
958:     }
959: 
960:    private:
961:     std::shared_ptr<AttemptManager> manager_;
962:     std::shared_ptr<BackendAttempt> attempt_;
963:     std::shared_ptr<BackendCallbackDrain> drain_;
964:   };
965: 
966:   struct CompletionPlan {
967:     bool finish{false};
968:     grpc::Status status;
969:     control::RequestOutcome outcome{control::RequestOutcome::kFailure};
970:     std::uint32_t attempt_count{0};
971:     std::array<std::uint32_t, 3> attempts_by_kind{};
972:     std::optional<AttemptKind> winner_kind;
973:     std::string winner_replica;
974:     std::array<std::shared_ptr<BackendAttempt>, kHardMaxTotalAttempts>
975:         cancel_attempts{};
976:     std::size_t cancel_count{0};
977:   };
978: 
979: #if defined(ARTC_ENABLE_TEST_HOOKS)
980:   [[nodiscard]] testing::TimerKey timer_key(testing::TimerKind kind) noexcept {
```

### Lines 1150-1280
```cpp
1150:     // The current logical request owns one route permit; don't mistake that
1151:     // permit alone for cluster-wide pressure when the route limit is one.
1152:     if (!snapshot || !admission.accepting || admission.limit == 0 ||
1153:         (admission.inflight > 1 && admission.inflight >= admission.limit)) return true;
1154:     const double target_us = static_cast<double>(
1155:         owner_->controller->config().aimd.target_latency.count());
1156:     const auto minimum_samples =
1157:         owner_->controller->config().health.minimum_latency_samples;
1158:     bool observed_latency = false;
1159:     for (const auto& replica : snapshot->replicas) {
1160:       if (replica.health == routing::HealthState::kUnavailable) continue;
1161:       double estimate = replica.latency_p95_us > 0.0
1162:                             ? replica.latency_p95_us
1163:                             : replica.latency_ewma_us;
1164:       if (replica.censored_latency_samples >= minimum_samples) {
1165:         estimate = std::max(estimate,
1166:                             replica.censored_latency_p95_lower_bound_us);
1167:       }
1168:       if (!std::isfinite(estimate) || estimate <= 0.0) return false;
1169:       observed_latency = true;
1170:       if (estimate <= target_us) return false;
1171:     }
1172:     return observed_latency;
1173:   }
1174: 
1175:   void schedule_hedge_locked(const std::weak_ptr<AttemptManager>& weak_self,
1176:                              const BackendAttempt& primary) {
1177:     if (!logical_.policy.hedging_enabled || hedge_started_ || !owner_->controller ||
1178:         owner_->dispatch_closed.load(std::memory_order_acquire) ||
1179:         owner_->shutdown_source.stop_requested() ||
1180:         state_ != LogicalState::kActive || primary.state != AttemptState::kInFlight ||
1181:         total_attempts_ >= total_attempt_limit_ ||
1182:         active_attempts_ >= owner_->attempt_config.max_active_attempts) return;
1183:     const auto delay = hedge_delay();
1184:     const auto remaining = logical_.context.remaining_budget(owner_->now());
1185:     const auto minimum = predicted_attempt_budget(primary.backend_index);
1186:     const auto delay_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(delay);
1187:     if (!retry_delay_fits_deadline(remaining, delay_ns, minimum)) {
1188:       saturating_add(owner_->metrics.hedge_deadline_denied, 1);
1189:       return;
1190:     }
1191:     auto until_fire = delay;
1192:     const auto elapsed = owner_->now() - primary.started_at;
1193:     if (elapsed > control::SteadyClock::duration::zero()) {
1194:       const auto already = std::chrono::duration_cast<std::chrono::microseconds>(elapsed);
1195:       until_fire = already >= delay ? std::chrono::microseconds::zero() : delay - already;
1196:     }
1197:     hedge_pending_ = true;
1198:     arm_timer(TimerSlot::kHedge, hedge_alarm_, until_fire,
1199:               [weak_self](bool ok) {
1200:                 if (ok) {
1201:                   if (const auto manager = weak_self.lock()) {
1202:                     manager->on_hedge_alarm();
1203:                   }
1204:                 }
1205:               });
1206:     saturating_add(owner_->metrics.pending_hedge_timers, 1);
1207:   }
1208: 
1209:   void on_hedge_alarm() noexcept {
1210: #if defined(ARTC_ENABLE_TEST_HOOKS)
1211:     checkpoint(testing::Checkpoint::kHedgeTimerReady, AttemptKind::kHedge);
1212: #endif
1213:     CompletionPlan plan;
1214:     try {
1215:       std::lock_guard lock(mutex_);
1216:       if (state_ != LogicalState::kActive || !hedge_pending_) return;
1217:       clear_hedge_pending_locked(false);
1218:       if (attempts_[0] == nullptr ||
1219:           attempts_[0]->state != AttemptState::kInFlight) return;
1220:       if (total_attempts_ >= total_attempt_limit_ ||
1221:           active_attempts_ >= owner_->attempt_config.max_active_attempts) {
1222:         saturating_add(owner_->metrics.hedge_attempt_limit, 1);
1223:         return;
1224:       }
1225:       if (overloaded_for_hedging_locked()) {
1226:         saturating_add(owner_->metrics.hedge_overload_denied, 1);
1227:         return;
1228:       }
1229:       std::shared_ptr<const control::ControllerSnapshot> snapshot;
1230:       std::size_t target = 0;
1231:       std::optional<routing::ReplicaLease> lease;
1232:       try {
1233:         const std::array excluded{primary_backend_index_};
1234:         lease.emplace(owner_->reserve_secondary(&target, &snapshot, excluded));
1235:       } catch (const NoHealthyReplica&) {
1236:         saturating_add(owner_->metrics.hedge_no_target, 1);
1237:         return;
1238:       }
1239:       if (target == primary_backend_index_) {
1240:         lease->release();
1241:         saturating_add(owner_->metrics.hedge_no_target, 1);
1242:         return;
1243:       }
1244:       if (overloaded_for_hedging_locked()) {
1245:         lease->release();
1246:         saturating_add(owner_->metrics.hedge_overload_denied, 1);
1247:         return;
1248:       }
1249:       const auto remaining = logical_.context.remaining_budget(owner_->now());
1250:       const auto required = predicted_target_budget(target);
1251:       if (!retry_delay_fits_deadline(remaining, std::chrono::nanoseconds::zero(), required)) {
1252:         lease->release();
1253:         saturating_add(owner_->metrics.hedge_deadline_denied, 1);
1254:         return;
1255:       }
1256:       const auto result = start_attempt_locked(AttemptKind::kHedge, target,
1257:                                                 std::move(*lease), required,
1258:                                                 &owner_->hedge_budget, &plan);
1259:       if (result == StartResult::kBudgetDenied) {
1260:         saturating_add(owner_->metrics.hedge_budget_denied, 1);
1261:       } else if (result == StartResult::kDeadlineDenied) {
1262:         saturating_add(owner_->metrics.hedge_deadline_denied, 1);
1263:       } else if (result == StartResult::kAtLimit) {
1264:         saturating_add(owner_->metrics.hedge_attempt_limit, 1);
1265:       }
1266:     } catch (...) {
1267:       CompletionPlan fallback;
1268:       {
1269:         std::lock_guard lock(mutex_);
1270:         saturating_add(owner_->metrics.hedge_dispatch_errors, 1);
1271:         const bool primary_can_continue =
1272:             state_ == LogicalState::kActive && attempts_[0] != nullptr &&
1273:             attempts_[0]->state == AttemptState::kInFlight && active_attempts_ == 1;
1274:         if (!primary_can_continue) {
1275:           finish_locked({grpc::StatusCode::INTERNAL, "hedge dispatch failed"},
1276:                         control::RequestOutcome::kFailure, nullptr, &fallback);
1277:         }
1278:       }
1279:       execute_plan(std::move(fallback));
1280:       return;
```

### Lines 1300-1530
```cpp
1300:     return nullptr;
1301:   }
1302: 
1303:   void advance_after_failure_locked(CompletionPlan* plan) {
1304:     if (state_ != LogicalState::kActive || active_attempts_ != 0) return;
1305:     cancel_hedge_locked();
1306:     if (owner_->dispatch_closed.load(std::memory_order_acquire) ||
1307:         owner_->shutdown_source.stop_requested()) {
1308:       finish_locked({grpc::StatusCode::CANCELLED, "router is shutting down"},
1309:                     control::RequestOutcome::kCancelled, nullptr, plan,
1310:                     AttemptCancellationReason::kShutdown);
1311:       return;
1312:     }
1313:     const std::size_t group_begin = retries_started_ == 0 ? 0 : total_attempts_ - 1;
1314:     const BackendAttempt* latest = nullptr;
1315:     const BackendAttempt* permanent = nullptr;
1316:     for (std::size_t index = group_begin; index < total_attempts_; ++index) {
1317:       const auto& attempt = attempts_[index];
1318:       if (!attempt || attempt->state == AttemptState::kSucceeded) continue;
1319:       if (!latest || attempt->id > latest->id) latest = attempt.get();
1320:       if (!is_retryable(logical_.policy, attempt->status.error_code()) &&
1321:           (!permanent || attempt->id < permanent->id)) {
1322:         permanent = attempt.get();
1323:       }
1324:     }
1325:     if (!latest) return;
1326:     if (permanent) {
1327:       if (logical_.policy.retry_enabled) {
1328:         saturating_add(owner_->metrics.retry_not_retryable, 1);
1329:       }
1330:       const auto outcome = permanent->status.error_code() ==
1331:                                    grpc::StatusCode::DEADLINE_EXCEEDED
1332:                                ? control::RequestOutcome::kTimeout
1333:                                : control::RequestOutcome::kFailure;
1334:       finish_locked(permanent->status, outcome, nullptr, plan);
1335:       return;
1336:     }
1337:     if (!logical_.policy.retry_enabled ||
1338:         logical_.policy.idempotency != Idempotency::kIdempotent ||
1339:         retries_started_ >= logical_.policy.max_retries ||
1340:         total_attempts_ >= total_attempt_limit_) {
1341:       if (logical_.policy.retry_enabled &&
1342:           logical_.policy.idempotency == Idempotency::kIdempotent &&
1343:           (retries_started_ >= logical_.policy.max_retries ||
1344:            total_attempts_ >= total_attempt_limit_)) {
1345:         saturating_add(owner_->metrics.retry_attempt_limit, 1);
1346:       }
1347:       const auto outcome = latest->status.error_code() ==
1348:                                    grpc::StatusCode::DEADLINE_EXCEEDED
1349:                                ? control::RequestOutcome::kTimeout
1350:                                : control::RequestOutcome::kFailure;
1351:       finish_locked(latest->status, outcome, nullptr, plan);
1352:       return;
1353:     }
1354: 
1355:     const auto backoff = retry_backoff_delay(
1356:         logical_.policy, retries_started_, retry_jitter());
1357:     const auto remaining = logical_.context.remaining_budget(owner_->now());
1358:     const auto required = predicted_attempt_budget(latest->backend_index);
1359:     const auto backoff_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(backoff);
1360:     if (!retry_delay_fits_deadline(remaining, backoff_ns, required)) {
1361:       saturating_add(owner_->metrics.retry_deadline_denied, 1);
1362:       if (remaining <= std::chrono::nanoseconds::zero()) {
1363:         finish_locked({grpc::StatusCode::DEADLINE_EXCEEDED,
1364:                        "logical request deadline expired before retry"},
1365:                       control::RequestOutcome::kDeadlineMiss, nullptr, plan,
1366:                       AttemptCancellationReason::kDeadline);
1367:       } else {
1368:         finish_locked(latest->status,
1369:                       control::RequestOutcome::kFailure, nullptr, plan);
1370:       }
1371:       return;
1372:     }
1373: 
1374:     retry_failure_id_ = latest->id;
1375:     retry_pending_ = true;
1376:     const auto weak_self = std::weak_ptr<AttemptManager>(
1377:         std::static_pointer_cast<AttemptManager>(shared_from_this()));
1378:     arm_timer(TimerSlot::kRetry, retry_alarm_, backoff,
1379:               [weak_self](bool ok) {
1380:                 if (ok) {
1381:                   if (const auto manager = weak_self.lock()) {
1382:                     manager->on_retry_alarm();
1383:                   }
1384:                 }
1385:               });
1386:     saturating_add(owner_->metrics.pending_retry_timers, 1);
1387:   }
1388: 
1389:   void on_retry_alarm() noexcept {
1390: #if defined(ARTC_ENABLE_TEST_HOOKS)
1391:     checkpoint(testing::Checkpoint::kRetryTimerReady, AttemptKind::kRetry);
1392: #endif
1393:     CompletionPlan plan;
1394:     try {
1395:       std::lock_guard lock(mutex_);
1396:       if (state_ != LogicalState::kActive || !retry_pending_) return;
1397:       clear_retry_pending_locked(false);
1398:       if (logical_deadline_expired_locked()) {
1399:         finish_locked({grpc::StatusCode::DEADLINE_EXCEEDED,
1400:                        "logical request deadline expired before retry"},
1401:                       control::RequestOutcome::kDeadlineMiss, nullptr, &plan,
1402:                       AttemptCancellationReason::kDeadline);
1403:       } else if (retry_failure_id_ == 0 || retry_failure_id_ > total_attempts_) {
1404:         finish_locked({grpc::StatusCode::INTERNAL, "retry state lost its failure"},
1405:                       control::RequestOutcome::kFailure, nullptr, &plan);
1406:       } else {
1407:         const auto& failed = attempts_[retry_failure_id_ - 1];
1408:         if (!failed || !is_retryable(logical_.policy, failed->status.error_code()) ||
1409:             active_attempts_ != 0 || total_attempts_ >= total_attempt_limit_ ||
1410:             retries_started_ >= logical_.policy.max_retries) {
1411:           if (failed && is_retryable(logical_.policy, failed->status.error_code()) &&
1412:               (total_attempts_ >= total_attempt_limit_ ||
1413:                retries_started_ >= logical_.policy.max_retries)) {
1414:             saturating_add(owner_->metrics.retry_attempt_limit, 1);
1415:           }
1416:           const auto* latest = latest_failure_locked();
1417:           if (latest) {
1418:             finish_locked(latest->status, control::RequestOutcome::kFailure,
1419:                           nullptr, &plan);
1420:           } else {
1421:             finish_locked({grpc::StatusCode::INTERNAL,
1422:                            "retry timer fired without a failed attempt"},
1423:                           control::RequestOutcome::kFailure, nullptr, &plan);
1424:           }
1425:         } else {
1426:           std::shared_ptr<const control::ControllerSnapshot> snapshot;
1427:           std::size_t target = 0;
1428:           std::optional<routing::ReplicaLease> lease;
1429:           bool same_replica_fallback = false;
1430:           bool have_target = false;
1431:           std::array<std::size_t, kHardMaxTotalAttempts> excluded{};
1432:           std::size_t excluded_count = 0;
1433:           for (std::size_t index = 0; index < total_attempts_; ++index) {
1434:             if (!attempts_[index]) continue;
1435:             const auto replica_index = attempts_[index]->backend_index;
1436:             const auto end = excluded.begin() +
1437:                              static_cast<std::ptrdiff_t>(excluded_count);
1438:             if (std::find(excluded.begin(), end, replica_index) == end) {
1439:               excluded[excluded_count++] = replica_index;
1440:             }
1441:           }
1442:           try {
1443:             lease.emplace(owner_->reserve_secondary(
1444:                 &target, &snapshot,
1445:                 std::span<const std::size_t>(excluded.data(), excluded_count)));
1446:             have_target = true;
1447:           } catch (const NoHealthyReplica&) {
1448:             if (!logical_.policy.allow_same_replica_retry) {
1449:               saturating_add(owner_->metrics.retry_no_target, 1);
1450:               finish_locked(failed->status, control::RequestOutcome::kFailure,
1451:                             nullptr, &plan);
1452:             } else {
1453:               try {
1454:                 lease.emplace(owner_->reserve_secondary(
1455:                     &target, &snapshot, std::span<const std::size_t>{}));
1456:                 const auto end = excluded.begin() +
1457:                                  static_cast<std::ptrdiff_t>(excluded_count);
1458:                 same_replica_fallback =
1459:                     std::find(excluded.begin(), end, target) != end;
1460:                 have_target = true;
1461:               } catch (const NoHealthyReplica&) {
1462:                 saturating_add(owner_->metrics.retry_no_target, 1);
1463:                 finish_locked(failed->status, control::RequestOutcome::kFailure,
1464:                               nullptr, &plan);
1465:               }
1466:             }
1467:           }
1468:           if (have_target) {
1469:             const auto end = excluded.begin() +
1470:                              static_cast<std::ptrdiff_t>(excluded_count);
1471:             same_replica_fallback = same_replica_fallback ||
1472:                                     std::find(excluded.begin(), end, target) != end;
1473:             const auto required = predicted_target_budget(target);
1474:             const auto remaining = logical_.context.remaining_budget(owner_->now());
1475:             if (!retry_delay_fits_deadline(remaining, std::chrono::nanoseconds::zero(),
1476:                                            required)) {
1477:               lease->release();
1478:               saturating_add(owner_->metrics.retry_deadline_denied, 1);
1479:               finish_locked(remaining <= std::chrono::nanoseconds::zero()
1480:                                 ? grpc::Status(grpc::StatusCode::DEADLINE_EXCEEDED,
1481:                                                "logical request deadline expired before retry")
1482:                                 : failed->status,
1483:                             remaining <= std::chrono::nanoseconds::zero()
1484:                                 ? control::RequestOutcome::kDeadlineMiss
1485:                                 : control::RequestOutcome::kFailure,
1486:                             nullptr, &plan);
1487:             } else {
1488:               const auto result = start_attempt_locked(AttemptKind::kRetry, target,
1489:                                                         std::move(*lease), required,
1490:                                                         &owner_->retry_budget, &plan);
1491:               if (result == StartResult::kBudgetDenied) {
1492:                 saturating_add(owner_->metrics.retry_budget_denied, 1);
1493:                 finish_locked(failed->status, control::RequestOutcome::kFailure,
1494:                               nullptr, &plan);
1495:               } else if (result == StartResult::kDeadlineDenied) {
1496:                 saturating_add(owner_->metrics.retry_deadline_denied, 1);
1497:                 finish_locked(failed->status, control::RequestOutcome::kFailure,
1498:                               nullptr, &plan);
1499:               } else if (result == StartResult::kStarted && same_replica_fallback) {
1500:                 saturating_add(owner_->metrics.retry_same_replica, 1);
1501:               } else if (result == StartResult::kAtLimit) {
1502:                 saturating_add(owner_->metrics.retry_attempt_limit, 1);
1503:                 finish_locked(failed->status, control::RequestOutcome::kFailure,
1504:                               nullptr, &plan);
1505:               }
1506:             }
1507:           }
1508:         }
1509:       }
1510:     } catch (...) {
1511:       fail_internal("retry dispatch failed");
1512:       return;
1513:     }
1514:     execute_plan(std::move(plan));
1515:   }
1516: 
1517:   template <typename Budget>
1518:   StartResult start_attempt_locked(AttemptKind kind, std::size_t backend_index,
1519:                                    routing::ReplicaLease lease,
1520:                                    std::chrono::nanoseconds required_budget,
1521:                                    Budget* budget, CompletionPlan* plan) {
1522:     if (state_ != LogicalState::kActive ||
1523:         total_attempts_ >= total_attempt_limit_ ||
1524:         active_attempts_ >= owner_->attempt_config.max_active_attempts) {
1525:       lease.release();
1526:       return StartResult::kAtLimit;
1527:     }
1528:     if (owner_->dispatch_closed.load(std::memory_order_acquire) ||
1529:         owner_->shutdown_source.stop_requested()) {
1530:       lease.release();
```

### Lines 1510-1645
```cpp
1510:     } catch (...) {
1511:       fail_internal("retry dispatch failed");
1512:       return;
1513:     }
1514:     execute_plan(std::move(plan));
1515:   }
1516: 
1517:   template <typename Budget>
1518:   StartResult start_attempt_locked(AttemptKind kind, std::size_t backend_index,
1519:                                    routing::ReplicaLease lease,
1520:                                    std::chrono::nanoseconds required_budget,
1521:                                    Budget* budget, CompletionPlan* plan) {
1522:     if (state_ != LogicalState::kActive ||
1523:         total_attempts_ >= total_attempt_limit_ ||
1524:         active_attempts_ >= owner_->attempt_config.max_active_attempts) {
1525:       lease.release();
1526:       return StartResult::kAtLimit;
1527:     }
1528:     if (owner_->dispatch_closed.load(std::memory_order_acquire) ||
1529:         owner_->shutdown_source.stop_requested()) {
1530:       lease.release();
1531:       finish_locked({grpc::StatusCode::CANCELLED, "router is shutting down"},
1532:                     control::RequestOutcome::kCancelled, nullptr, plan,
1533:                     AttemptCancellationReason::kShutdown);
1534:       return StartResult::kExpired;
1535:     }
1536: 
1537:     auto attempt = std::make_shared<BackendAttempt>(std::move(lease));
1538:     attempt->id = static_cast<std::uint64_t>(total_attempts_ + 1);
1539:     attempt->kind = kind;
1540:     attempt->backend_index = backend_index;
1541:     attempt->replica_id = attempt->lease.replica()->id;
1542:     attempt->request = logical_.request;
1543:     auto reactor = std::make_unique<BackendReactor>(
1544:         std::static_pointer_cast<AttemptManager>(shared_from_this()), attempt);
1545: 
1546:     auto system_now = std::chrono::system_clock::now();
1547:     auto steady_now = owner_->now();
1548:     auto remaining = logical_.context.remaining_budget(steady_now);
1549:     if (owner_->controller && remaining <= std::chrono::nanoseconds::zero()) {
1550:       attempt->lease.release();
1551:       finish_locked({grpc::StatusCode::DEADLINE_EXCEEDED,
1552:                      "logical request deadline expired before backend dispatch"},
1553:                     control::RequestOutcome::kDeadlineMiss, nullptr, plan,
1554:                     AttemptCancellationReason::kDeadline);
1555:       return StartResult::kExpired;
1556:     }
1557:     if (required_budget > std::chrono::nanoseconds::zero() &&
1558:         !retry_delay_fits_deadline(remaining, std::chrono::nanoseconds::zero(),
1559:                                    required_budget)) {
1560:       attempt->lease.release();
1561:       return StartResult::kDeadlineDenied;
1562:     }
1563:     if (context_->IsCancelled()) {
1564:       attempt->lease.release();
1565:       finish_cancelled_locked(plan);
1566:       return StartResult::kExpired;
1567:     }
1568: 
1569:     // Serialize dispatch commitment with router shutdown. Shutdown closes this
1570:     // gate before notifying managers, so an attempt either starts before that
1571:     // boundary or is rejected without reaching the backend.
1572: #if defined(ARTC_ENABLE_TEST_HOOKS)
1573:     checkpoint(testing::Checkpoint::kBeforeDispatchFence, kind);
1574: #endif
1575:     std::lock_guard dispatch_lock(owner_->dispatch_mutex);
1576:     if (owner_->dispatch_closed.load(std::memory_order_acquire) ||
1577:         owner_->shutdown_source.stop_requested()) {
1578:       attempt->lease.release();
1579:       finish_locked({grpc::StatusCode::CANCELLED, "router is shutting down"},
1580:                     control::RequestOutcome::kCancelled, nullptr, plan,
1581:                     AttemptCancellationReason::kShutdown);
1582:       return StartResult::kExpired;
1583:     }
1584:     system_now = std::chrono::system_clock::now();
1585:     steady_now = owner_->now();
1586:     remaining = logical_.context.remaining_budget(steady_now);
1587:     if (owner_->controller && remaining <= std::chrono::nanoseconds::zero()) {
1588:       attempt->lease.release();
1589:       finish_locked({grpc::StatusCode::DEADLINE_EXCEEDED,
1590:                      "logical request deadline expired before backend dispatch"},
1591:                     control::RequestOutcome::kDeadlineMiss, nullptr, plan,
1592:                     AttemptCancellationReason::kDeadline);
1593:       return StartResult::kExpired;
1594:     }
1595:     if (required_budget > std::chrono::nanoseconds::zero() &&
1596:         !retry_delay_fits_deadline(remaining, std::chrono::nanoseconds::zero(),
1597:                                    required_budget)) {
1598:       attempt->lease.release();
1599:       return StartResult::kDeadlineDenied;
1600:     }
1601:     if (context_->IsCancelled()) {
1602:       attempt->lease.release();
1603:       finish_cancelled_locked(plan);
1604:       return StartResult::kExpired;
1605:     }
1606:     if constexpr (!std::is_same_v<Budget, NoBudget>) {
1607:       if (budget != nullptr && !budget->try_consume(steady_now)) {
1608:         attempt->lease.release();
1609:         return StartResult::kBudgetDenied;
1610:       }
1611:     }
1612: 
1613:     attempt->started_at = steady_now;
1614:     attempt->deadline = logical_.context.effective_deadline;
1615:     attempt->state = AttemptState::kInFlight;
1616:     if (owner_->controller) {
1617:       attempt->context.set_deadline(
1618:           logical_.context.downstream_deadline(steady_now, system_now));
1619:     } else {
1620:       attempt->context.set_deadline(context_->deadline());
1621:     }
1622:     attempts_[total_attempts_++] = attempt;
1623:     ++active_attempts_;
1624:     owner_->metrics.active_attempts.fetch_add(1, std::memory_order_relaxed);
1625:     saturating_add(owner_->metrics.attempts[attempt_kind_index(kind)], 1);
1626:     if (owner_->controller) owner_->controller->record_backend_attempt(backend_index);
1627:     auto& stub = owner_->backends[backend_index].stub;
1628:     reactor->register_for_drain();
1629:     stub->async()->Execute(&attempt->context, &attempt->request,
1630:                            &attempt->response, reactor.get());
1631:     reactor->StartCall();
1632:     static_cast<void>(reactor.release());
1633: 
1634:     if (kind == AttemptKind::kPrimary) {
1635:       const auto weak_self = std::weak_ptr<AttemptManager>(
1636:           std::static_pointer_cast<AttemptManager>(shared_from_this()));
1637:       schedule_hedge_locked(weak_self, *attempt);
1638:     } else if (kind == AttemptKind::kHedge) {
1639:       hedge_started_ = true;
1640:       saturating_add(owner_->metrics.hedge_started, 1);
1641:     } else {
1642:       ++retries_started_;
1643:       saturating_add(owner_->metrics.retry_started, 1);
1644:     }
1645: #if defined(ARTC_ENABLE_TEST_HOOKS)
```

### Lines 1660-1735
```cpp
1660:                   deadline_expired ? control::RequestOutcome::kDeadlineMiss
1661:                                    : control::RequestOutcome::kCancelled,
1662:                   nullptr, plan,
1663:                   deadline_expired ? AttemptCancellationReason::kDeadline
1664:                                    : AttemptCancellationReason::kCaller);
1665:   }
1666: 
1667:   bool finish_locked(grpc::Status status, control::RequestOutcome outcome,
1668:                      const BackendAttempt* winner, CompletionPlan* plan,
1669:                      AttemptCancellationReason cancel_reason =
1670:                          AttemptCancellationReason::kInternal) {
1671:     if (state_ != LogicalState::kActive) return false;
1672:     if (outcome == control::RequestOutcome::kFailure &&
1673:         status.error_code() == grpc::StatusCode::DEADLINE_EXCEEDED) {
1674:       outcome = control::RequestOutcome::kTimeout;
1675:     }
1676:     state_ = LogicalState::kCompleted;
1677:     saturating_add(owner_->metrics.logical_terminals, 1);
1678:     completion_time_ = owner_->now();
1679:     logical_.context.terminal_outcome = outcome;
1680:     if (owner_->controller) owner_->controller->record_logical_completion(outcome);
1681:     cancel_hedge_locked();
1682:     cancel_retry_locked();
1683:     cancel_timer(TimerSlot::kDeadline, deadline_alarm_);
1684:     plan->finish = true;
1685:     plan->status = std::move(status);
1686:     plan->outcome = outcome;
1687:     plan->attempt_count = static_cast<std::uint32_t>(total_attempts_);
1688:     if (winner != nullptr) {
1689:       response_->CopyFrom(winner->response);
1690:       response_->set_backend_attempt_count(static_cast<std::uint32_t>(total_attempts_));
1691:       plan->winner_kind = winner->kind;
1692:       plan->winner_replica = winner->replica_id;
1693:       saturating_add(owner_->metrics.winners[attempt_kind_index(winner->kind)], 1);
1694:     }
1695:     for (std::size_t index = 0; index < total_attempts_; ++index) {
1696:       const auto& attempt = attempts_[index];
1697:       if (attempt) ++plan->attempts_by_kind[attempt_kind_index(attempt->kind)];
1698:       if (!attempt || attempt.get() == winner ||
1699:           attempt->state != AttemptState::kInFlight || attempt->cancel_requested) continue;
1700:       attempt->cancel_requested = true;
1701:       attempt->cancellation_reason =
1702:           winner != nullptr ? AttemptCancellationReason::kWinner : cancel_reason;
1703:       attempt->cancellation_requested_at = completion_time_;
1704:       attempt->censored_by_winner =
1705:           winner != nullptr && outcome == control::RequestOutcome::kSuccess;
1706:       const auto elapsed_us = std::chrono::duration_cast<std::chrono::microseconds>(
1707:           completion_time_ - attempt->started_at).count();
1708:       if (elapsed_us > 0) {
1709:         saturating_add(owner_->metrics.wasted_attempt_time_us,
1710:                        static_cast<std::uint64_t>(elapsed_us));
1711:       }
1712:       saturating_add(owner_->metrics.cancelled_attempts, 1);
1713:       saturating_add(owner_->metrics.cancellations_by_reason[
1714:                          cancellation_reason_index(attempt->cancellation_reason)], 1);
1715:       plan->cancel_attempts[plan->cancel_count++] = attempt;
1716:     }
1717:     primary_lease_.release();
1718:     release_permit_if_drained_locked();
1719:     return true;
1720:   }
1721: 
1722:   void release_permit_if_drained_locked() noexcept {
1723:     if (state_ == LogicalState::kCompleted && active_attempts_ == 0) {
1724:       permit_.release();
1725:     }
1726:   }
1727: 
1728:   void cancel_hedge_locked() {
1729:     clear_hedge_pending_locked(true);
1730:   }
1731: 
1732:   void cancel_retry_locked() {
1733:     clear_retry_pending_locked(true);
1734:   }
1735: 
```

### Lines 1945-1975
```cpp
1945:                              control::ControllerConfig controller_config,
1946:                              std::unordered_map<std::string, MethodPolicy>
1947:                                  method_policies,
1948:                              AttemptRuntimeConfig attempt_config)
1949:     : state_(std::make_shared<State>(std::move(replicas), policy, seed,
1950:                                      ewma_smoothing, std::move(controller_config),
1951:                                      std::move(method_policies),
1952:                                      std::move(attempt_config))) {}
1953: 
1954: RouterService::~RouterService() { begin_shutdown(); }
1955: 
1956: void RouterService::begin_shutdown() noexcept {
1957:   if (!state_) return;
1958:   {
1959:     std::unique_lock dispatch_lock(state_->dispatch_mutex, std::defer_lock);
1960: #if defined(ARTC_ENABLE_TEST_HOOKS)
1961:     if (state_->test_control && !dispatch_lock.try_lock()) {
1962:       state_->test_checkpoint(testing::Checkpoint::kShutdownFenceContended);
1963:     }
1964: #endif
1965:     if (!dispatch_lock.owns_lock()) dispatch_lock.lock();
1966:     state_->dispatch_closed.store(true, std::memory_order_release);
1967:   }
1968: #if defined(ARTC_ENABLE_TEST_HOOKS)
1969:   state_->test_checkpoint(testing::Checkpoint::kShutdownFenceClosed);
1970: #endif
1971:   state_->shutdown_source.request_stop();
1972:   if (state_->controller) state_->controller->close_admission();
1973: }
1974: 
1975: AttemptSnapshot RouterService::attempt_snapshot() const noexcept {
```

### Lines 2170-2200
```cpp
2170:       if (state_->adaptive_selector) scores = state_->adaptive_selector->explain(*snapshot, state_->states);
2171:       for (std::size_t i = 0; i < snapshot->replicas.size(); ++i) {
2172:         if (i != 0) sample << ';';
2173:         const auto& replica = snapshot->replicas[i];
2174:         sample << i << ':' << health_name(replica.health) << ':' << replica.inflight
2175:                << ':' << replica.latency_ewma_us << ':' << replica.error_ewma;
2176:         if (i < scores.size()) sample << ':' << scores[i].total;
2177:       }
2178:       decision_metadata = sample.str();
2179:     }
2180:     LogicalRequest logical{std::move(request_context), method_policy, *request};
2181:     auto state = std::make_shared<AttemptManager>(
2182:         reactor, context, response, state_, std::move(logical), std::move(lease),
2183:         std::move(permit), backend_index, std::move(decision_metadata));
2184:     reactor->bind(state);
2185:     state->start();
2186:   } catch (const NoHealthyReplica&) {
2187:     permit.release();
2188:     return reject(control::AdmissionResult::kNoHealthyReplica,
2189:                   grpc::StatusCode::UNAVAILABLE, "no healthy replica available");
2190:   } catch (const std::exception& error) {
2191:     permit.release();
2192:     if (controller) {
2193:       context->AddTrailingMetadata("artc-admission-result", "REJECT_INTERNAL");
2194:       context->AddTrailingMetadata("artc-backend-attempts", "0");
2195:     }
2196:     reactor->Finish({grpc::StatusCode::INTERNAL, error.what()});
2197:   }
2198:   return reactor;
2199: }
2200: 
```

## src/control/phase2.cc

### Lines 650-710
```cpp
650:       increment_saturated(&controller_overload_events_);
651:     } else {
652:       const auto room = config_.aimd.max_limit - current;
653:       next = current + std::min(config_.aimd.additive_increase, room);
654:     }
655:     if (next != current) {
656:       gate_.set_limit(next);
657:       increment_saturated(&controller_limit_changes_);
658:     }
659:   }
660: 
661:   last_tick_ = now;
662:   publish(now);
663: }
664: 
665: void Phase2Controller::update_health(SteadyTime now,
666:     const std::vector<routing::ReplicaWindow>& windows,
667:     const std::vector<routing::ReplicaStats>& stats) {
668:   double baseline = std::numeric_limits<double>::infinity();
669:   for (std::size_t i = 0; i < stats.size(); ++i) {
670:     if (stats[i].health != routing::HealthState::kUnavailable &&
671:         stats[i].latency_samples >= config_.health.minimum_latency_samples &&
672:         windows[i].succeeded != 0 &&
673:         windows[i].latency_p95_us > 0.0) {
674:       baseline = std::min(baseline, windows[i].latency_p95_us);
675:     }
676:   }
677: 
678:   for (std::size_t i = 0; i < replicas_.size(); ++i) {
679:     auto health = stats[i].health;
680:     auto& progress = health_progress_[i];
681:     if (health != routing::HealthState::kUnavailable &&
682:         stats[i].consecutive_failures >= config_.health.consecutive_failures_to_unavailable) {
683:       health = routing::HealthState::kUnavailable;
684:       progress = {};
685:     } else if (health == routing::HealthState::kUnavailable) {
686:       if (stats[i].has_failure &&
687:           now >= saturating_add(stats[i].last_failure, config_.health.recovery_cooldown)) {
688:         health = routing::HealthState::kRecovering;
689:         progress = {};
690:       }
691:     } else if (health == routing::HealthState::kRecovering) {
692:       if (windows[i].failed != 0) {
693:         health = routing::HealthState::kUnavailable;
694:         progress.recovery_successes = 0;
695:       } else {
696:         const auto remaining = config_.health.recovery_successes -
697:             std::min(progress.recovery_successes, config_.health.recovery_successes);
698:         progress.recovery_successes +=
699:             std::min(remaining, windows[i].useful_successes);
700:         if (progress.recovery_successes >= config_.health.recovery_successes) {
701:           health = routing::HealthState::kHealthy;
702:           progress = {};
703:         }
704:       }
705:     } else if (std::isfinite(baseline) && baseline > 0.0 &&
706:                stats[i].latency_samples >= config_.health.minimum_latency_samples &&
707:                windows[i].succeeded != 0) {
708:       const double ratio = windows[i].latency_p95_us / baseline;
709:       if (ratio >= config_.health.degraded_latency_ratio) {
710:         progress.slow_windows = std::min<std::uint64_t>(2, progress.slow_windows + 1);
```

## tests/integration/attempt_management_test.cc

### Lines 315-370
```cpp
315:     std::this_thread::sleep_for(1ms);
316:     snapshot = router.attempt_snapshot();
317:   }
318:   router.begin_shutdown();
319:   router_server->Shutdown();
320:   primary_server->Shutdown();
321:   hedge_server->Shutdown();
322:   return {status, snapshot, std::move(response)};
323: }
324: 
325: TEST(AttemptManagerIntegrationTest, CallbackDrainWaitsThroughBackendOnDone) {
326:   HeldBackend backend("A1");
327:   int backend_port = 0;
328:   auto backend_server = artc::rpc::start_server("127.0.0.1:0", backend, &backend_port);
329:   artc::rpc::RouterService router(
330:       {{"A1", address_for(backend_port)}}, artc::routing::Policy::kRoundRobin,
331:       17, 0.2, controller_config());
332:   int router_port = 0;
333:   auto router_server = artc::rpc::start_server("127.0.0.1:0", router, &router_port);
334: 
335:   grpc::ClientContext context;
336:   context.set_deadline(std::chrono::system_clock::now() + 2s);
337:   artc::v1::WorkRequest request;
338:   artc::v1::WorkResponse response;
339:   grpc::Status status;
340:   std::thread caller([&] {
341:     status = execute(address_for(router_port), request, &response, &context);
342:   });
343:   if (!backend.wait_for_calls(1, 1s)) {
344:     context.TryCancel();
345:     caller.join();
346:     FAIL() << "backend did not receive the request";
347:     return;
348:   }
349:   EXPECT_FALSE(router.wait_for_attempt_callbacks(std::chrono::steady_clock::now() + 10ms));
350:   const auto backend_call = backend.call(0);
351:   if (!backend_call || !backend_call->complete()) {
352:     context.TryCancel();
353:     caller.join();
354:     FAIL() << "backend call could not be completed";
355:     return;
356:   }
357:   caller.join();
358:   EXPECT_TRUE(status.ok()) << status.error_message();
359:   EXPECT_TRUE(router.wait_for_attempt_callbacks(std::chrono::steady_clock::now() + 1s));
360:   EXPECT_EQ(router.attempt_snapshot().pending_backend_callbacks, 0U);
361: 
362:   router.begin_shutdown();
363:   router_server->Shutdown();
364:   backend_server->Shutdown();
365: }
366: 
367: TEST(AttemptManagerIntegrationTest, HedgeWinsOnDistinctTargetAndCancelsLoser) {
368:   HeldBackend primary("A1");
369:   HeldBackend hedge("A2");
370:   int slow_port = 0;
```

### Lines 730-875
```cpp
730:   healthy_server->Shutdown();
731: 
732:   EXPECT_EQ(status.error_code(), grpc::StatusCode::UNAVAILABLE);
733:   EXPECT_EQ(snapshot.backend_attempts_total[0], 1U);
734:   EXPECT_EQ(snapshot.backend_attempts_total[2], 0U);
735:   EXPECT_EQ(snapshot.retry_started_total, 0U);
736:   EXPECT_EQ(snapshot.retry_budget_denied_total, 1U);
737:   EXPECT_EQ(snapshot.retry_budget.denied_total, 1U);
738:   EXPECT_EQ(snapshot.attempt_amplification, 1.0);
739: }
740: 
741: TEST(AttemptManagerIntegrationTest, HedgeIsSuppressedWhenEveryReplicaIsSlow) {
742:   artc::rpc::ServiceA a1("A1", 40'000, {});
743:   artc::rpc::ServiceA a2("A2", 40'000, {});
744:   int a1_port = 0;
745:   int a2_port = 0;
746:   auto a1_server = artc::rpc::start_server("127.0.0.1:0", a1, &a1_port);
747:   auto a2_server = artc::rpc::start_server("127.0.0.1:0", a2, &a2_port);
748:   auto config = controller_config();
749:   config.aimd.target_latency = 10ms;
750:   config.aimd.control_interval = 1ms;
751:   artc::rpc::MethodPolicy method;
752:   method.idempotency = artc::rpc::Idempotency::kIdempotent;
753:   method.hedging_enabled = true;
754:   method.max_total_attempts = 2;
755:   method.hedge_delay_min = 2ms;
756:   method.hedge_delay_max = 200ms;
757:   artc::rpc::AttemptRuntimeConfig attempts;
758:   attempts.hedge_budget = {.capacity = 4, .refill_per_second = 0.0};
759:   artc::rpc::RouterService router(
760:       {{"A1", address_for(a1_port)}, {"A2", address_for(a2_port)}},
761:       artc::routing::Policy::kAdaptiveConcurrencyOnly, 17, 0.2,
762:       config, {{"/artc.v1.Traffic/Execute", method}}, attempts);
763:   int router_port = 0;
764:   auto router_server = artc::rpc::start_server("127.0.0.1:0", router,
765:                                                 &router_port);
766: 
767:   std::array<grpc::Status, 3> statuses;
768:   for (std::size_t index = 0; index < statuses.size(); ++index) {
769:     grpc::ClientContext context;
770:     context.set_deadline(std::chrono::system_clock::now() + 2s);
771:     artc::v1::WorkRequest request;
772:     request.set_request_id(index + 1);
773:     request.set_work_units(index == 2 ? 2 : 1);
774:     artc::v1::WorkResponse response;
775:     statuses[index] = execute(address_for(router_port), request, &response, &context);
776:     if (index == 1) {
777:       EXPECT_TRUE(wait_until([&] {
778:         const auto snapshot = router.controller_snapshot();
779:         return snapshot && snapshot->replicas.size() == 2 &&
780:                std::all_of(snapshot->replicas.begin(), snapshot->replicas.end(),
781:                            [](const auto& replica) {
782:                              return replica.latency_samples > 0 &&
783:                                     replica.latency_p95_us >= 10'000.0;
784:                            });
785:       }, 1s));
786:     }
787:   }
788:   const auto snapshot = router.attempt_snapshot();
789:   router.begin_shutdown();
790:   router_server->Shutdown();
791:   a1_server->Shutdown();
792:   a2_server->Shutdown();
793: 
794:   for (const auto& status : statuses) EXPECT_TRUE(status.ok()) << status.error_message();
795:   EXPECT_EQ(snapshot.backend_attempts_total[0], 3U);
796:   EXPECT_EQ(snapshot.backend_attempts_total[1], 0U);
797:   EXPECT_EQ(snapshot.hedge_started_total, 0U);
798:   EXPECT_EQ(snapshot.hedge_overload_denied_total, 1U);
799:   EXPECT_EQ(snapshot.attempt_amplification, 1.0);
800: }
801: 
802: TEST(AttemptManagerIntegrationTest, HedgeOverloadUsesLiveAdmissionPressure) {
803:   HeldBackend a1("A1");
804:   HeldBackend a2("A2");
805:   HeldBackend a3("A3");
806:   std::array<HeldBackend*, 3> backends{&a1, &a2, &a3};
807:   std::array<int, 3> ports{};
808:   std::array<std::unique_ptr<grpc::Server>, 3> backend_servers;
809:   std::array<std::size_t, 3> first_indices{};
810:   for (std::size_t index = 0; index < backends.size(); ++index) {
811:     backend_servers[index] = artc::rpc::start_server(
812:         "127.0.0.1:0", *backends[index], &ports[index]);
813:   }
814: 
815:   auto config = controller_config();
816:   config.aimd.initial_limit = 2;
817:   config.aimd.max_limit = 2;
818:   config.aimd.control_interval = 10s;
819:   artc::rpc::MethodPolicy method;
820:   method.idempotency = artc::rpc::Idempotency::kIdempotent;
821:   method.hedging_enabled = true;
822:   method.max_total_attempts = 2;
823:   method.hedge_delay_min = 250ms;
824:   method.hedge_delay_max = 250ms;
825:   artc::rpc::AttemptRuntimeConfig attempts;
826:   attempts.hedge_budget = {.capacity = 4, .refill_per_second = 0.0};
827:   attempts.retry_budget = {.capacity = 0, .refill_per_second = 0.0};
828:   artc::rpc::RouterService router(
829:       {{"A1", address_for(ports[0])}, {"A2", address_for(ports[1])},
830:        {"A3", address_for(ports[2])}},
831:       artc::routing::Policy::kArtcAdaptiveNoDeadline, 17, 0.2, config,
832:       {{"/artc.v1.Traffic/Execute", method}}, attempts);
833:   int router_port = 0;
834:   auto router_server = artc::rpc::start_server("127.0.0.1:0", router,
835:                                                 &router_port);
836: 
837:   std::array<grpc::ClientContext, 2> contexts;
838:   std::array<artc::v1::WorkResponse, 2> responses;
839:   std::array<grpc::Status, 2> statuses;
840:   std::array<std::thread, 2> callers;
841:   bool primary_calls_entered = true;
842:   for (std::size_t index = 0; index < contexts.size(); ++index) {
843:     contexts[index].set_deadline(std::chrono::system_clock::now() + 3s);
844:     artc::v1::WorkRequest request;
845:     request.set_request_id(index + 1);
846:     callers[index] = std::thread([&, index, request] {
847:       statuses[index] = execute(address_for(router_port), request,
848:                                  &responses[index], &contexts[index]);
849:     });
850:     if (!wait_for_new_calls(backends, first_indices, index + 1U, 2s)) {
851:       primary_calls_entered = false;
852:       break;
853:     }
854:   }
855:   if (!primary_calls_entered) {
856:     for (auto& context : contexts) context.TryCancel();
857:     for (const auto& call : calls_since(backends, first_indices)) {
858:       call->complete({grpc::StatusCode::CANCELLED, "test cleanup"});
859:     }
860:   }
861: 
862:   const bool both_denied = wait_until([&] {
863:     return router.attempt_snapshot().hedge_overload_denied_total == 2;
864:   }, 3s);
865:   const auto published = router.controller_snapshot();
866:   auto calls = calls_since(backends, first_indices);
867:   for (const auto& call : calls) call->complete();
868:   for (auto& caller : callers) {
869:     if (caller.joinable()) caller.join();
870:   }
871:   static_cast<void>(wait_until([&] { return router.attempt_snapshot().active_attempts == 0; }));
872:   const auto snapshot = router.attempt_snapshot();
873: 
874:   router.begin_shutdown();
875:   router_server->Shutdown();
```

### Lines 1100-1240
```cpp
1100:   const auto controller_snapshot = router.controller_snapshot();
1101:   router.begin_shutdown();
1102:   router_server->Shutdown();
1103:   backend_server->Shutdown();
1104: 
1105:   EXPECT_TRUE(entered);
1106:   EXPECT_TRUE(completed);
1107:   EXPECT_EQ(status.error_code(), grpc::StatusCode::DEADLINE_EXCEEDED);
1108:   EXPECT_EQ(attempt_snapshot.retry_budget_denied_total, 1U);
1109:   EXPECT_EQ(attempt_snapshot.retry_started_total, 0U);
1110:   EXPECT_EQ(attempt_snapshot.backend_attempts_total[0], 1U);
1111:   EXPECT_EQ(attempt_snapshot.backend_attempts_total[2], 0U);
1112:   ASSERT_NE(controller_snapshot, nullptr);
1113:   EXPECT_EQ(controller_snapshot->deadline_misses, 1U);
1114:   EXPECT_EQ(controller_snapshot->deadline_goodput, 0U);
1115:   ASSERT_EQ(controller_snapshot->replicas.size(), 1U);
1116:   EXPECT_EQ(controller_snapshot->replicas[0].timed_out_total, 1U);
1117: }
1118: 
1119: TEST(AttemptManagerIntegrationTest, DeadlineSuppressesHedgeAndRetry) {
1120:   {
1121:     HeldBackend a1("A1");
1122:     HeldBackend a2("A2");
1123:     int a1_port = 0;
1124:     int a2_port = 0;
1125:     auto a1_server = artc::rpc::start_server("127.0.0.1:0", a1, &a1_port);
1126:     auto a2_server = artc::rpc::start_server("127.0.0.1:0", a2, &a2_port);
1127:     auto config = controller_config();
1128:     config.default_deadline = 200ms;
1129:     artc::rpc::MethodPolicy method;
1130:     method.idempotency = artc::rpc::Idempotency::kIdempotent;
1131:     method.hedging_enabled = true;
1132:     method.max_total_attempts = 2;
1133:     method.hedge_delay_min = 250ms;
1134:     method.hedge_delay_max = 250ms;
1135:     artc::rpc::AttemptRuntimeConfig attempts;
1136:     attempts.minimum_attempt_budget = 20ms;
1137:     artc::rpc::RouterService router(
1138:         {{"A1", address_for(a1_port)}, {"A2", address_for(a2_port)}},
1139:       artc::routing::Policy::kArtcAdaptiveNoDeadline, 17, 0.2,
1140:         config, {{"/artc.v1.Traffic/Execute", method}}, attempts);
1141:     int router_port = 0;
1142:     auto router_server = artc::rpc::start_server("127.0.0.1:0", router,
1143:                                                   &router_port);
1144:     grpc::ClientContext context;
1145:     context.set_deadline(std::chrono::system_clock::now() + 200ms);
1146:     artc::v1::WorkRequest request;
1147:     artc::v1::WorkResponse response;
1148:     grpc::Status status;
1149:     std::thread caller([&] {
1150:       status = execute(address_for(router_port), request, &response, &context);
1151:     });
1152:     const bool primary_entered = a1.wait_for_calls(1, 2s);
1153:     const bool hedge_denied = wait_until([&] {
1154:       return router.attempt_snapshot().hedge_deadline_denied_total == 1U;
1155:     });
1156:     caller.join();
1157:     a1.complete_all({grpc::StatusCode::CANCELLED, "deadline test cleanup"});
1158:     a2.complete_all({grpc::StatusCode::CANCELLED, "deadline test cleanup"});
1159:     static_cast<void>(wait_until([&] {
1160:       return router.attempt_snapshot().active_attempts == 0U;
1161:     }));
1162:     const auto snapshot = router.attempt_snapshot();
1163:     router.begin_shutdown();
1164:     router_server->Shutdown();
1165:     a1_server->Shutdown();
1166:     a2_server->Shutdown();
1167:     EXPECT_TRUE(primary_entered);
1168:     EXPECT_TRUE(hedge_denied);
1169:     EXPECT_EQ(status.error_code(), grpc::StatusCode::DEADLINE_EXCEEDED);
1170:     EXPECT_EQ(snapshot.backend_attempts_total[1], 0U);
1171:     EXPECT_EQ(snapshot.hedge_deadline_denied_total, 1U);
1172:   }
1173: 
1174:   {
1175:     HeldBackend transient("A1");
1176:     HeldBackend healthy("A2");
1177:     int transient_port = 0;
1178:     int healthy_port = 0;
1179:     auto transient_server = artc::rpc::start_server("127.0.0.1:0", transient,
1180:                                                      &transient_port);
1181:     auto healthy_server = artc::rpc::start_server("127.0.0.1:0", healthy,
1182:                                                    &healthy_port);
1183:     artc::rpc::MethodPolicy method;
1184:     method.idempotency = artc::rpc::Idempotency::kIdempotent;
1185:     method.retry_enabled = true;
1186:     method.max_total_attempts = 2;
1187:     method.max_retries = 1;
1188:     method.retry_backoff_base = 500ms;
1189:     method.retry_backoff_max = 500ms;
1190:     method.retry_jitter_max = 0ms;
1191:     auto config = controller_config();
1192:     config.default_deadline = 300ms;
1193:     artc::rpc::RouterService router(
1194:         {{"A1", address_for(transient_port)}, {"A2", address_for(healthy_port)}},
1195:       artc::routing::Policy::kArtcAdaptiveNoDeadline, 17, 0.2,
1196:         config, {{"/artc.v1.Traffic/Execute", method}});
1197:     int router_port = 0;
1198:     auto router_server = artc::rpc::start_server("127.0.0.1:0", router,
1199:                                                   &router_port);
1200:     grpc::ClientContext context;
1201:     context.set_deadline(std::chrono::system_clock::now() + 300ms);
1202:     artc::v1::WorkRequest request;
1203:     artc::v1::WorkResponse response;
1204:     grpc::Status status;
1205:     std::thread caller([&] {
1206:       status = execute(address_for(router_port), request, &response, &context);
1207:     });
1208:     const bool primary_entered = transient.wait_for_calls(1, 2s);
1209:     const auto primary_call = transient.call(0);
1210:     const bool primary_failed = primary_call && primary_call->complete(
1211:         {grpc::StatusCode::UNAVAILABLE, "injected transient failure"});
1212:     const bool retry_denied = wait_until([&] {
1213:       return router.attempt_snapshot().retry_deadline_denied_total == 1U;
1214:     });
1215:     caller.join();
1216:     transient.complete_all({grpc::StatusCode::CANCELLED, "retry test cleanup"});
1217:     healthy.complete_all({grpc::StatusCode::CANCELLED, "retry test cleanup"});
1218:     const auto snapshot = router.attempt_snapshot();
1219:     router.begin_shutdown();
1220:     router_server->Shutdown();
1221:     transient_server->Shutdown();
1222:     healthy_server->Shutdown();
1223:     EXPECT_TRUE(primary_entered);
1224:     EXPECT_TRUE(primary_failed);
1225:     EXPECT_TRUE(retry_denied);
1226:     EXPECT_EQ(status.error_code(), grpc::StatusCode::UNAVAILABLE);
1227:     EXPECT_EQ(snapshot.backend_attempts_total[2], 0U);
1228:     EXPECT_EQ(snapshot.retry_deadline_denied_total, 1U);
1229:     EXPECT_EQ(snapshot.retry_budget.consumed_total, 0U);
1230:   }
1231: }
1232: 
1233: TEST(AttemptManagerIntegrationTest, NonIdempotentUnavailableDoesNotRetryOrHedge) {
1234:   artc::rpc::ServiceA transient("A1", 0, {}, 1);
1235:   artc::rpc::ServiceA healthy("A2", 0, {});
1236:   int transient_port = 0;
1237:   int healthy_port = 0;
1238:   auto transient_server = artc::rpc::start_server("127.0.0.1:0", transient,
1239:                                                  &transient_port);
1240:   auto healthy_server = artc::rpc::start_server("127.0.0.1:0", healthy,
```

### Lines 1580-1635
```cpp
1580:   primary_server->Shutdown();
1581:   hedge_server->Shutdown();
1582: 
1583:   ASSERT_TRUE(primary_entered);
1584:   EXPECT_TRUE(hedge_timer_armed);
1585:   EXPECT_EQ(status.error_code(), grpc::StatusCode::CANCELLED);
1586:   EXPECT_FALSE(late_hedge);
1587:   EXPECT_EQ(hedge.call_count(), 0U);
1588:   EXPECT_EQ(snapshot.backend_attempts_total[0], 1U);
1589:   EXPECT_EQ(snapshot.backend_attempts_total[1], 0U);
1590:   EXPECT_EQ(snapshot.logical_terminal_transitions_total, 1U);
1591:   EXPECT_EQ(snapshot.pending_hedge_timers, 0U);
1592: }
1593: 
1594: TEST(AttemptManagerIntegrationTest, ShutdownCancelsPendingRetryTimer) {
1595:   HeldBackend transient("A1");
1596:   int backend_port = 0;
1597:   auto backend_server = artc::rpc::start_server("127.0.0.1:0", transient,
1598:                                                  &backend_port);
1599:   artc::rpc::MethodPolicy method;
1600:   method.idempotency = artc::rpc::Idempotency::kIdempotent;
1601:   method.retry_enabled = true;
1602:   method.allow_same_replica_retry = true;
1603:   method.max_total_attempts = 2;
1604:   method.max_retries = 1;
1605:   method.retry_backoff_base = 500ms;
1606:   method.retry_backoff_max = 500ms;
1607:   method.retry_jitter_max = 0ms;
1608:   artc::rpc::RouterService router(
1609:       {{"A1", address_for(backend_port)}},
1610:       artc::routing::Policy::kArtcAdaptiveNoDeadline, 17, 0.2,
1611:       controller_config(), {{"/artc.v1.Traffic/Execute", method}});
1612:   int router_port = 0;
1613:   auto router_server = artc::rpc::start_server("127.0.0.1:0", router,
1614:                                                 &router_port);
1615: 
1616:   grpc::ClientContext context;
1617:   context.set_deadline(std::chrono::system_clock::now() + 3s);
1618:   artc::v1::WorkRequest request;
1619:   artc::v1::WorkResponse response;
1620:   grpc::Status status;
1621:   std::thread caller([&] {
1622:     status = execute(address_for(router_port), request, &response, &context);
1623:   });
1624:   const bool first_attempt_entered = transient.wait_for_calls(1, 2s);
1625:   const auto first_attempt = transient.call(0);
1626:   const bool first_attempt_failed = first_attempt && first_attempt->complete(
1627:       {grpc::StatusCode::UNAVAILABLE, "transient backend failure"});
1628:   const bool retry_timer_armed = wait_until([&] {
1629:     const auto snapshot = router.attempt_snapshot();
1630:     return snapshot.attempt_completions_total[0] == 1U &&
1631:            snapshot.pending_retry_timers == 1U;
1632:   });
1633:   router.begin_shutdown();
1634:   caller.join();
1635:   const bool late_retry = transient.wait_for_calls(2, 600ms);
```

## lab/run_phase2_experiments.sh

### Lines 1-180
```cpp
1: #!/usr/bin/env bash
2: set -Eeuo pipefail
3: 
4: ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
5: cd "$ROOT"
6: 
7: RUN_ID="${ARTC_RUN_ID:-$(date -u +%Y%m%dt%H%M%Sz)-$$}"
8: PHASE2_START_AT="${ARTC_PHASE2_START_AT:-A}"
9: if [[ ! "$RUN_ID" =~ ^[a-z0-9][a-z0-9_-]*$ ]]; then
10:   echo "invalid ARTC_RUN_ID" >&2
11:   exit 2
12: fi
13: if [[ "$PHASE2_START_AT" != A && "$PHASE2_START_AT" != I && \
14:       "$PHASE2_START_AT" != DEADLINE ]]; then
15:   echo "ARTC_PHASE2_START_AT must be A, I, or DEADLINE" >&2
16:   exit 2
17: fi
18: PROJECT="${COMPOSE_PROJECT_NAME:-artc-phase2-$RUN_ID}"
19: OUT_DIR="$ROOT/artifacts/runs/$RUN_ID/phase2"
20: COMPOSE_FILE="lab/compose.yaml"
21: mkdir -p "$OUT_DIR"
22: if [[ "$PHASE2_START_AT" == I ]]; then
23:   for label in A-healthy B-a2-straggler C-a2-cpu-saturation D-a2-down \
24:                E-a2-recovery F-downstream-b-slow G-one-to-five-burst H-global-overload; do
25:     python3 lab/validate_artifacts.py --allow-errors "$OUT_DIR/$label"
26:   done
27: fi
28: if [[ "$PHASE2_START_AT" == DEADLINE ]]; then
29:   for label in A-healthy B-a2-straggler C-a2-cpu-saturation D-a2-down \
30:                E-a2-recovery F-downstream-b-slow G-one-to-five-burst \
31:                H-global-overload I-all-replicas-slow J-load-normal-again \
32:                stability-00-30-normal stability-30-60-step-up \
33:                stability-60-90-sustained stability-90-120-recovery \
34:                recovery-00-30-healthy recovery-30-60-a2-unavailable \
35:                recovery-60-120-a2-reintegrated; do
36:     python3 lab/validate_artifacts.py --allow-errors "$OUT_DIR/$label"
37:   done
38: fi
39: 
40: export COMPOSE_PROJECT_NAME="$PROJECT"
41: export ARTC_GIT_REVISION="$(git rev-parse HEAD)"
42: export ARTC_SERVICE_B_DELAY_US=0
43: if [[ -n "$(git status --porcelain=v1 --untracked-files=all)" ]]; then
44:   export ARTC_WORKTREE_DIRTY=true
45: else
46:   export ARTC_WORKTREE_DIRTY=false
47: fi
48: 
49: LAB_UP=false
50: FAULT_SERVICES=()
51: STRESS_ACTIVE=false
52: LOAD_PID=""
53: MONITOR_PID=""
54: 
55: compose() { docker compose -f "$COMPOSE_FILE" "$@"; }
56: 
57: clear_faults() {
58:   local service
59:   for service in "${FAULT_SERVICES[@]}"; do
60:     compose exec -T "$service" tc qdisc del dev eth0 root >/dev/null 2>&1 || true
61:   done
62:   FAULT_SERVICES=()
63: }
64: 
65: stop_stress() {
66:   if [[ "$STRESS_ACTIVE" == true ]]; then
67:     compose exec -T a2 pkill -TERM -x stress-ng >/dev/null 2>&1 || true
68:     STRESS_ACTIVE=false
69:   fi
70: }
71: 
72: on_exit() {
73:   local result=$?
74:   trap - EXIT INT TERM
75:   if [[ -n "$LOAD_PID" ]]; then
76:     kill "$LOAD_PID" >/dev/null 2>&1 || true
77:     wait "$LOAD_PID" >/dev/null 2>&1 || true
78:   fi
79:   if [[ -n "$MONITOR_PID" ]]; then
80:     kill "$MONITOR_PID" >/dev/null 2>&1 || true
81:     wait "$MONITOR_PID" >/dev/null 2>&1 || true
82:   fi
83:   stop_stress || result=1
84:   if [[ "$LAB_UP" == true ]]; then
85:     clear_faults || result=1
86:     compose down --remove-orphans || result=1
87:     LAB_UP=false
88:   fi
89:   exit "$result"
90: }
91: trap on_exit EXIT
92: trap 'exit 130' INT
93: trap 'exit 143' TERM
94: 
95: check_health() {
96:   local target="$1"
97:   compose run --rm --no-deps loadgen --health --target "$target"
98: }
99: 
100: start_policy() {
101:   local policy="$1"
102:   local sample_every=0
103:   if [[ "$#" -ge 2 ]]; then
104:     sample_every="$2"
105:   elif [[ "$policy" == artc_* || "$policy" == adaptive_concurrency_only ]]; then
106:     sample_every="${ARTC_PHASE2_SAMPLE_EVERY:-100}"
107:   fi
108:   export ARTC_ROUTING_POLICY="$policy"
109:   export ARTC_DECISION_SAMPLE_EVERY="$sample_every"
110:   compose up -d --no-deps --force-recreate --wait --wait-timeout 60 router
111:   check_health router:50050
112: }
113: 
114: monitor_router() {
115:   local container_id="$1"
116:   local load_pid="$2"
117:   local output="$3"
118:   (
119:     while kill -0 "$load_pid" >/dev/null 2>&1; do
120:       printf '%s,' "$(date -u +%s)"
121:       docker stats --no-stream --format '{{.CPUPerc}},{{.MemUsage}},{{.PIDs}}' \
122:         "$container_id" 2>/dev/null || true
123:       sleep "${ARTC_STATS_INTERVAL_SEC:-1}"
124:     done
125:   ) >"$output" &
126:   MONITOR_PID=$!
127: }
128: 
129: run_load() {
130:   local label="$1"
131:   local target="$2"
132:   local mode="$3"
133:   local rate="$4"
134:   local duration_ms="$5"
135:   local deadline_ms="$6"
136:   local initial_rate="${7:-0}"
137:   local allow_uninstrumented="${8:-false}"
138:   local max_issue_lag_us="${9:-${ARTC_PHASE2_MAX_ISSUE_LAG_US:-20000}}"
139:   local run_dir="$OUT_DIR/$label"
140:   local loadgen_log="$OUT_DIR/$label.loadgen.stdout"
141:   local container_id
142:   local target_service="${target%%:*}"
143:   mkdir -p "$run_dir"
144:   export ARTC_RUN_ID="$RUN_ID-$label"
145:   export ARTC_ROUTING_POLICY="${ARTC_ROUTING_POLICY:-round_robin}"
146:   container_id="$(compose ps -q "$target_service")"
147:   if [[ -z "$container_id" ]]; then
148:     echo "target container is missing: $target_service" >&2
149:     return 1
150:   fi
151: 
152:   compose run --rm --no-deps loadgen \
153:     --target "$target" \
154:     --output "/artifacts/$RUN_ID/phase2/$label" \
155:     --mode "$mode" \
156:     --duration-ms "$duration_ms" \
157:     --rate-rps "$rate" \
158:     --initial-rate-rps "$initial_rate" \
159:     --max-inflight "${ARTC_LOADGEN_MAX_INFLIGHT:-8192}" \
160:     --max-issue-lag-us "$max_issue_lag_us" \
161:     --deadline-ms "$deadline_ms" \
162:     --invoke-dependency --allow-errors \
163:     >"$loadgen_log" 2>&1 &
164:   LOAD_PID=$!
165:   monitor_router "$container_id" "$LOAD_PID" "$OUT_DIR/$label.router-resources.csv"
166:   local result=0
167:   wait "$LOAD_PID" || result=$?
168:   LOAD_PID=""
169:   kill "$MONITOR_PID" >/dev/null 2>&1 || true
170:   wait "$MONITOR_PID" >/dev/null 2>&1 || true
171:   MONITOR_PID=""
172:   if (( result != 0 )); then
173:     cat "$loadgen_log" >&2
174:     echo "loadgen failed label=$label status=$result" >&2
175:     return "$result"
176:   fi
177:   local validation=(python3 lab/validate_artifacts.py --allow-errors)
178:   if [[ "$allow_uninstrumented" == true ]]; then
179:     validation+=(--allow-uninstrumented)
180:   fi
```

## lab/run_phase2_soak.sh

### Lines 1-125
```cpp
1: #!/usr/bin/env bash
2: set -Eeuo pipefail
3: 
4: ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
5: cd "$ROOT"
6: RUN_ID="${ARTC_RUN_ID:-$(date -u +%Y%m%dt%H%M%Sz)-$$}"
7: if [[ ! "$RUN_ID" =~ ^[a-z0-9][a-z0-9_-]*$ ]]; then
8:   echo "invalid ARTC_RUN_ID" >&2
9:   exit 2
10: fi
11: PROJECT="${COMPOSE_PROJECT_NAME:-artc-phase2-soak-$RUN_ID}"
12: OUT_DIR="$ROOT/artifacts/runs/$RUN_ID/phase2-soak"
13: mkdir -p "$OUT_DIR"
14: export COMPOSE_PROJECT_NAME="$PROJECT"
15: export ARTC_RUN_ID="$RUN_ID-soak"
16: export ARTC_ROUTING_POLICY=artc_adaptive
17: export ARTC_DECISION_SAMPLE_EVERY="${ARTC_PHASE2_SAMPLE_EVERY:-100}"
18: export ARTC_GIT_REVISION="$(git rev-parse HEAD)"
19: if [[ -n "$(git status --porcelain=v1 --untracked-files=all)" ]]; then
20:   export ARTC_WORKTREE_DIRTY=true
21: else
22:   export ARTC_WORKTREE_DIRTY=false
23: fi
24: 
25: COMPOSE_FILE="lab/compose.yaml"
26: LAB_UP=false
27: LOAD_PID=""
28: MONITOR_PID=""
29: compose() { docker compose -f "$COMPOSE_FILE" "$@"; }
30: 
31: on_exit() {
32:   local result=$?
33:   trap - EXIT INT TERM
34:   if [[ -n "$LOAD_PID" ]]; then
35:     kill "$LOAD_PID" >/dev/null 2>&1 || true
36:     wait "$LOAD_PID" >/dev/null 2>&1 || true
37:   fi
38:   if [[ -n "$MONITOR_PID" ]]; then
39:     kill "$MONITOR_PID" >/dev/null 2>&1 || true
40:     wait "$MONITOR_PID" >/dev/null 2>&1 || true
41:   fi
42:   if [[ "$LAB_UP" == true ]]; then
43:     compose down --remove-orphans || result=1
44:     LAB_UP=false
45:   fi
46:   exit "$result"
47: }
48: trap on_exit EXIT
49: trap 'exit 130' INT
50: trap 'exit 143' TERM
51: 
52: if ! docker image inspect artc-phase1:local >/dev/null 2>&1; then
53:   echo "missing artc-phase1:local; run lab/run_phase1_gates.sh first" >&2
54:   exit 2
55: fi
56: docker info >/dev/null
57: LAB_UP=true
58: compose up -d --wait --wait-timeout 90
59: compose run --rm --no-deps loadgen --health --target router:50050
60: 
61: duration_ms="${ARTC_SOAK_DURATION_MS:-1800000}"
62: rate_rps="${ARTC_SOAK_RATE_RPS:-500}"
63: container_id="$(compose ps -q router)"
64: run_dir="$OUT_DIR/run"
65: loadgen_log="$OUT_DIR/loadgen.stdout"
66: mkdir -p "$run_dir"
67: 
68: compose run --rm --no-deps loadgen \
69:   --target router:50050 \
70:   --output "/artifacts/$RUN_ID/phase2-soak/run" \
71:   --mode constant \
72:   --duration-ms "$duration_ms" \
73:   --rate-rps "$rate_rps" \
74:   --max-inflight "${ARTC_LOADGEN_MAX_INFLIGHT:-8192}" \
75:   --max-issue-lag-us "${ARTC_PHASE2_MAX_ISSUE_LAG_US:-20000}" \
76:   --deadline-ms 5000 --invoke-dependency --allow-errors \
77:   >"$loadgen_log" 2>&1 &
78: LOAD_PID=$!
79: 
80: (
81:   echo "unix_s,fd_count,thread_count,socket_count,rss_kib,cpu_percent,docker_rss,pids"
82:   while kill -0 "$LOAD_PID" >/dev/null 2>&1; do
83:     compose exec -T router sh -c '
84:       pid="$(pgrep -xo artc_lab_node)"
85:       fd="$(find "/proc/$pid/fd" -mindepth 1 -maxdepth 1 | wc -l)"
86:       threads="$(sed -n "s/^Threads:[[:space:]]*//p" "/proc/$pid/status")"
87:       sockets="$(find "/proc/$pid/fd" -mindepth 1 -maxdepth 1 -lname "socket:*" | wc -l)"
88:       rss="$(sed -n "s/^VmRSS:[[:space:]]*//p" "/proc/$pid/status")"
89:       printf "%s,%s,%s,%s,%s\n" "$(date +%s)" "$fd" "$threads" "$sockets" "$rss"
90:     ' 2>/dev/null | while IFS=, read -r timestamp fd threads sockets rss; do
91:       cpu_memory="$(docker stats --no-stream --format '{{.CPUPerc}},{{.MemUsage}},{{.PIDs}}' "$container_id" 2>/dev/null || true)"
92:       IFS=, read -r cpu memory pids <<<"$cpu_memory"
93:       printf '%s,%s,%s,%s,%s,%s,%s,%s\n' \
94:         "$timestamp" "$fd" "$threads" "$sockets" "${rss%% *}" \
95:         "$cpu" "${memory%% / *}" "$pids"
96:     done
97:     sleep "${ARTC_SOAK_SAMPLE_INTERVAL_SEC:-10}"
98:   done
99: ) >"$OUT_DIR/resources.csv" &
100: MONITOR_PID=$!
101: 
102: result=0
103: wait "$LOAD_PID" || result=$?
104: LOAD_PID=""
105: kill "$MONITOR_PID" >/dev/null 2>&1 || true
106: wait "$MONITOR_PID" >/dev/null 2>&1 || true
107: MONITOR_PID=""
108: if (( result != 0 )); then
109:   cat "$loadgen_log" >&2
110:   echo "soak loadgen failed status=$result" >&2
111:   exit "$result"
112: fi
113: 
114: python3 lab/validate_artifacts.py --allow-errors "$run_dir"
115: python3 lab/analyze_phase2_resources.py "$OUT_DIR" >"$OUT_DIR/resource-analysis.stdout"
116: echo "phase2_soak_complete=$OUT_DIR"
```

## src/bench/loadgen_main.cc

### Lines 510-540
```cpp
510:       if (attempts && *attempts != 0) ++invariant_violations;
511:       if (complete_phase3_counts &&
512:           (*primary_attempt_count != 0 || *hedge_attempt_count != 0 ||
513:            *retry_attempt_count != 0)) {
514:         ++invariant_violations;
515:       }
516:     }
517:     if (!attempts) ++invariant_violations;
518:   }
519:   if (attempts) {
520:     ++attempt_metadata_observed;
521:     backend_attempts += *attempts;
522:     if (*attempts != 0 && !selected_replica.empty()) ++by_replica[selected_replica];
523:   } else if (status.ok()) {
524:     backend_attempts += response.backend_attempt_count();
525:     if (!response.replica_id().empty()) ++by_replica[response.replica_id()];
526:   }
527:   if (!decision.empty()) {
528:     constexpr std::size_t kMaximumDecisionSamples = 10'000;
529:     if (decision_samples.size() < kMaximumDecisionSamples) {
530:       const auto elapsed_us = static_cast<std::uint64_t>(
531:           std::chrono::duration_cast<std::chrono::microseconds>(issued_at - started).count());
532:       decision_samples.emplace_back(elapsed_us, std::move(decision));
533:     } else {
534:       ++decision_samples_dropped;
535:     }
536:   }
537:   const bool rejected_before_dispatch = admission.starts_with("REJECT_");
538:   if (rpc_elapsed > deadline ||
539:       (status.error_code() == grpc::StatusCode::DEADLINE_EXCEEDED &&
540:        !rejected_before_dispatch)) {
```
