def some_pick($name):
  .["mbt::nondetPicks"][$name] as $pick
  | if $pick.tag == "Some"
    then $pick.value.tag
    else error("missing nondeterministic pick: " + $name)
    end;

def writer:
  some_pick("writer")
  | if . == "Writer1" then "writer-1"
    elif . == "Writer2" then "writer-2"
    else error("unknown writer variant: " + .)
    end;

def object_id:
  some_pick("objectId")
  | if . == "Object1" then "object-1"
    elif . == "Object2" then "object-2"
    else error("unknown object variant: " + .)
    end;

def attempt_id:
  some_pick("attemptId")
  | if . == "Attempt1" then "attempt-1"
    elif . == "Attempt2" then "attempt-2"
    elif . == "Attempt3" then "attempt-3"
    elif . == "Attempt4" then "attempt-4"
    elif . == "Attempt5" then "attempt-5"
    elif . == "Attempt6" then "attempt-6"
    else error("unknown attempt variant: " + .)
    end;

def commit_mode:
  some_pick("mode")
  | if . == "Confirmed" then "confirmed"
    elif . == "AmbiguousLanded" then "ambiguous-landed"
    elif . == "AmbiguousDropped" then "ambiguous-dropped"
    else error("unknown commit mode variant: " + .)
    end;

{
  source: {
    format: "ITF",
    spec: ."#meta".source,
    seed: $seed,
    loop: (.loop // null)
  },
  operations: [
    .states[1:][]
    | . as $state
    | {
        index: $state."#meta".index,
        op:
          (if $state."mbt::actionTaken" == "chooseAcquire" then "acquire"
           elif $state."mbt::actionTaken" == "choosePublish" then "publish"
           elif $state."mbt::actionTaken" == "chooseCommit" then "commit-attempt"
           elif $state."mbt::actionTaken" == "chooseRelease" then "release-attempt"
           else error("unknown MBT action: " + $state."mbt::actionTaken")
           end)
      }
      + if $state."mbt::actionTaken" == "chooseAcquire" then
          {writer: ($state | writer)}
        elif $state."mbt::actionTaken" == "choosePublish" then
          {
            writer: ($state | writer),
            object: ($state | object_id),
            attempt: ($state | attempt_id)
          }
        elif $state."mbt::actionTaken" == "chooseCommit" then
          {
            writer: ($state | writer),
            object: ($state | object_id),
            attempt: ($state | attempt_id),
            mode: ($state | commit_mode)
          }
        elif $state."mbt::actionTaken" == "chooseRelease" then
          {writer: ($state | writer)}
        else
          error("unreachable MBT action")
        end
  ]
}
