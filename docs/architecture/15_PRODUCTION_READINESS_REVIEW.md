# 15 — Production Readiness Review (PRR)

Use this before every release candidate. Every answer must be backed by code, test, artifact, or explicit limitation.

## Request safety

- Can one logical request ever expose two terminal results?
- Can a rejected/cancelled/expired request start new backend work?
- Can a non-idempotent call ever hedge or retry unexpectedly?
- Can an attempt outlive all state it references?
- Is every permit/token released/accounted exactly once?

## Amplification

- What is the mathematical maximum total attempts per logical call?
- What is the system-wide maximum hedge/retry rate under failure?
- What happens when every replica slows at once?
- Can retries and hedges trigger each other recursively?

## Overload

- Which queues exist and what bounds each one?
- What happens at 2x, 5x, 10x capacity?
- Does latency remain low only because ARTC rejects most traffic?
- Is goodput reported alongside latency?

## Concurrency/lifecycle

- Which callbacks can run concurrently?
- What synchronizes each shared field?
- Which race interleavings are deterministic tests?
- Is TSan clean for realistic lifecycle stress?
- What happens during shutdown at every lifecycle stage?

## Failure/recovery

- Does every documented failure have a test?
- Does every degradation scenario include recovery?
- Can recovered replicas regain traffic?
- Can flapping cause uncontrolled oscillation?
- What happens if controller observations disappear or become stale?

## Resources

- What bounds memory, FDs, sockets, threads, timers, and telemetry queues?
- Does a long soak show drift?
- What happens under allocator/resource pressure?
- Does telemetry outage alter correctness?

## Measurement

- Is the generator open-loop for headline tests?
- Is coordinated omission addressed?
- Can the generator itself sustain the requested schedule?
- Are p999 sample counts adequate?
- Are results repeated and raw histograms retained?
- Are baseline and ARTC conditions identical except for intended policy differences?

## Operational behavior

- What is the startup dependency contract?
- What is the maximum graceful-shutdown time?
- What happens when replicas appear late or restart?
- Is invalid config rejected before serving traffic?
- Can observability components be absent?

## Interview/publication readiness

- Can the architecture be drawn in 60 seconds?
- Are major tradeoffs documented?
- Is there at least one real bug/root-cause story?
- Are negative results included?
- Can every resume claim be traced to raw evidence?
- Are limitations explicit rather than hidden?

## Release decision

A release is not "production-style ready" while a critical question above has an unknown answer.
