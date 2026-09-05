import quasar_jobs/retention

const day = 86_400_000

pub fn policy_materialises_only_enabled_rules_test() {
  let policy =
    retention.new()
    |> retention.completed_for(days: 7)
    |> retention.discarded_for(days: 30)

  assert retention.valid(policy)
  assert retention.rules(policy, 40 * day)
    == [
      retention.Completed(33 * day),
      retention.Discarded(10 * day),
    ]
}

pub fn policy_rejects_non_positive_durations_test() {
  assert retention.new()
    |> retention.cancelled_for(days: 0)
    |> retention.valid
    == False
}
