def model_var:
  [.vars[] | select(type == "string" and endswith("::state"))] as $names
  | if ($names | length) == 1
    then $names[0]
    else error("each ITF trace must contain exactly one Durable model state")
    end;

def outcome($event):
  if $event == null then empty
  elif $event.tag == "Acquired" then "Acquired"
  elif $event.tag == "Published" then "Published"
  elif $event.tag == "CommitAttempted" then $event.value.result.tag
  elif $event.tag == "ReleaseAttempted" then
    if $event.value.accepted then "ReleaseAccepted" else "ReleaseRejected" end
  else error("unknown Durable model event: \($event.tag)")
  end;

def counts:
  sort
  | group_by(.)
  | map({key: .[0], value: length})
  | from_entries;

def required_actions:
  ["chooseAcquire", "choosePublish", "chooseCommit", "chooseRelease"];

def required_outcomes:
  ["Acquired", "Published", "Committed", "Reconciled", "LeaseFenced",
   "ObjectUnverified", "CommitAmbiguous", "ReleaseAccepted",
   "ReleaseRejected"];

map(. as $trace
    | ($trace | model_var) as $model_var
    | $trace.states[]
    | {action: ."mbt::actionTaken",
       outcome: outcome(.[$model_var].events[-1]?)}) as $observations
| ($observations | map(.action) | counts) as $action_counts
| ($observations | map(.outcome) | counts) as $outcome_counts
| (required_actions) as $required_actions
| (required_outcomes) as $required_outcomes
| {
    schema: "jolt-chdb/durable-itf-corpus-coverage-v1",
    generator: {step: "legacyStep", seed: $seed, max_steps: 6},
    trace_count: $trace_count,
    transition_count: ($observations | length),
    required: {
      actions: $required_actions,
      outcomes: $required_outcomes
    },
    observed: {
      action_counts: $action_counts,
      outcome_counts: $outcome_counts
    },
    missing: {
      actions: ($required_actions - ($action_counts | keys)),
      outcomes: ($required_outcomes - ($outcome_counts | keys))
    }
  }
