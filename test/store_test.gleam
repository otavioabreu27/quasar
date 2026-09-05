import quasar_jobs/store
import quasar_jobs/store/memory
import store_contract

pub fn memory_fences_every_acknowledgement_test() {
  let assert Ok(database) = memory.new()
  store_contract.fencing(database, "fencing")
  assert store.close(database) == Ok(Nil)
}

pub fn memory_validates_and_discards_exhausted_leases_test() {
  let assert Ok(database) = memory.new()
  store_contract.validation_and_exhaustion(database, "exhaustion")
  assert store.close(database) == Ok(Nil)
}
