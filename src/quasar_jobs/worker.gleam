//// Typed durable worker definitions.

import quasar_jobs/job.{type ExecutionToken, type JobId, type NewJob}

pub type Context {
  Context(job_id: JobId, attempt: Int, execution_token: ExecutionToken)
}

pub type CompletionMode {
  RuntimeManaged
  WorkerManaged
}

pub opaque type Worker(input) {
  Worker(
    name: String,
    encode: fn(input) -> String,
    decode: fn(String) -> Result(input, String),
    perform: fn(input, Context) -> Result(Nil, String),
    completion_mode: CompletionMode,
  )
}

pub opaque type Definition {
  Definition(
    name: String,
    run: fn(String, Context) -> Result(Nil, String),
    completion_mode: CompletionMode,
  )
}

/// Defines a typed durable worker without macros.
pub fn new(
  name name: String,
  encode encode: fn(input) -> String,
  decode decode: fn(String) -> Result(input, String),
  perform perform: fn(input, Context) -> Result(Nil, String),
) -> Worker(input) {
  Worker(name:, encode:, decode:, perform:, completion_mode: RuntimeManaged)
}

/// Marks a worker as responsible for atomically persisting its own completion.
///
/// Store adapters use this for transactional workers. The callback must not
/// return `Ok` until its business effect and fenced completion are committed.
pub fn with_managed_completion(worker: Worker(input)) -> Worker(input) {
  Worker(..worker, completion_mode: WorkerManaged)
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
  Definition(
    worker.name,
    fn(payload, context) {
      case worker.decode(payload) {
        Error(error) -> Error("decode: " <> error)
        Ok(input) -> worker.perform(input, context)
      }
    },
    worker.completion_mode,
  )
}

@internal
pub fn definition_name(definition: Definition) -> String {
  definition.name
}

@internal
pub fn completion_mode(definition: Definition) -> CompletionMode {
  definition.completion_mode
}

@internal
pub fn run(
  definition: Definition,
  payload: String,
  context: Context,
) -> Result(Nil, String) {
  definition.run(payload, context)
}
