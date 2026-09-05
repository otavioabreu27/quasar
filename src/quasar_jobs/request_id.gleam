//// Correlation identity shared by local execution and HTTP adapters.

pub opaque type RequestId {
  RequestId(String)
}

pub fn new() -> RequestId {
  RequestId(unique_id())
}

pub fn from_string(value: String) -> RequestId {
  RequestId(value)
}

pub fn to_string(id: RequestId) -> String {
  let RequestId(value) = id
  value
}

@external(erlang, "quasar_jobs_ffi", "unique_id")
fn unique_id() -> String
