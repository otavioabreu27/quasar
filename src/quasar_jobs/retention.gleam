//// Database-agnostic retention policy for terminal durable jobs.

import gleam/list
import gleam/option.{type Option, None, Some}

const milliseconds_per_day = 86_400_000

pub opaque type Policy {
  Policy(
    completed_ms: Option(Int),
    cancelled_ms: Option(Int),
    discarded_ms: Option(Int),
  )
}

pub type Rule {
  Completed(before: Int)
  Cancelled(before: Int)
  Discarded(before: Int)
}

/// Creates a policy that retains every terminal job indefinitely.
pub fn new() -> Policy {
  Policy(None, None, None)
}

/// Deletes completed jobs after the configured number of days.
pub fn completed_for(policy: Policy, days days: Int) -> Policy {
  Policy(..policy, completed_ms: Some(days * milliseconds_per_day))
}

/// Deletes cancelled jobs after the configured number of days.
pub fn cancelled_for(policy: Policy, days days: Int) -> Policy {
  Policy(..policy, cancelled_ms: Some(days * milliseconds_per_day))
}

/// Deletes discarded jobs after the configured number of days.
pub fn discarded_for(policy: Policy, days days: Int) -> Policy {
  Policy(..policy, discarded_ms: Some(days * milliseconds_per_day))
}

/// Returns true when every configured duration is positive.
pub fn valid(policy: Policy) -> Bool {
  positive(policy.completed_ms)
  && positive(policy.cancelled_ms)
  && positive(policy.discarded_ms)
}

/// Materialises enabled rules using one common clock snapshot.
pub fn rules(policy: Policy, now: Int) -> List(Rule) {
  []
  |> append_rule(policy.completed_ms, fn(duration) { Completed(now - duration) })
  |> append_rule(policy.cancelled_ms, fn(duration) { Cancelled(now - duration) })
  |> append_rule(policy.discarded_ms, fn(duration) { Discarded(now - duration) })
}

pub fn status(rule: Rule) -> String {
  case rule {
    Completed(_) -> "completed"
    Cancelled(_) -> "cancelled"
    Discarded(_) -> "discarded"
  }
}

pub fn cutoff(rule: Rule) -> Int {
  case rule {
    Completed(before) | Cancelled(before) | Discarded(before) -> before
  }
}

fn positive(duration: Option(Int)) -> Bool {
  case duration {
    None -> True
    Some(milliseconds) -> milliseconds > 0
  }
}

fn append_rule(
  rules: List(Rule),
  duration: Option(Int),
  make: fn(Int) -> Rule,
) {
  case duration {
    None -> rules
    Some(milliseconds) -> list.append(rules, [make(milliseconds)])
  }
}
