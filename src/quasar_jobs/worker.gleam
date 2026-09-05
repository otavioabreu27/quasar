//// Typed durable worker definitions.

import quasar_jobs/job.{type JobId, type NewJob}

pub type Context {
  Context(job_id: JobId, attempt: Int)
}

pub opaque type Worker(input) {
  Worker(
    name: String,
    encode: fn(input) -> String,
    decode: fn(String) -> Result(input, String),
    perform: fn(input, Context) -> Result(Nil, String),
  )
}

pub opaque type Definition {
  Definition(name: String, run: fn(String, Context) -> Result(Nil, String))
}

/// Defines a typed durable worker without macros.
pub fn new(
  name name: String,
  encode encode: fn(input) -> String,
  decode decode: fn(String) -> Result(input, String),
  perform perform: fn(input, Context) -> Result(Nil, String),
) -> Worker(input) {
  Worker(name:, encode:, decode:, perform:)
}

/// Encodes typed input into a persistable job payload.
pub fn job(worker: Worker(input), input: input) -> NewJob {
  job.new_job(worker.name, worker.encode(input), 0, 3)
}

pub fn name(worker: Worker(input)) -> String {
  worker.name
}

@internal
pub fn erase(worker: Worker(input)) -> Definition {
  Definition(worker.name, fn(payload, context) {
    case worker.decode(payload) {
      Error(error) -> Error("decode: " <> error)
      Ok(input) -> worker.perform(input, context)
    }
  })
}

@internal
pub fn definition_name(definition: Definition) -> String {
  definition.name
}

@internal
pub fn run(
  definition: Definition,
  payload: String,
  context: Context,
) -> Result(Nil, String) {
  definition.run(payload, context)
}
