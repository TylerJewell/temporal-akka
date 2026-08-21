# Acknowledgements

This project is a port of **[temporalio/temporal](https://github.com/temporalio/temporal)**.

- **Licence and copyright.** `temporalio/temporal` is licensed under the MIT
  License. Its `LICENSE` names two copyright holders: "Copyright (c) 2025 Temporal
  Technologies Inc. All rights reserved." and "Copyright (c) 2020 Uber Technologies,
  Inc.".
- **Was anything copied verbatim?** No source was copied. Three kinds of string are
  reproduced exactly, deliberately, because they are part of the behaviour being
  compared rather than part of the implementation:
  - the refusal messages `encounter history size being zero`,
    `Unknown event type: <type>`, `unable to get activity info`,
    `unable to get child workflow info`, `unable to find workflow task: <id>`,
    `Cannot apply events for new run when current run is still running`,
    `cannot update version history with a lower version <v>. Last version: <last>` and
    `cannot add version history with a lower event id <id>. Last event id: <last>`
    (SPEC-001 §3 R1, R15b, R18, R20, R26);
  - the history event type names, which are the source's own enumeration spellings
    minus the `EVENT_TYPE_` prefix;
  - the search-attribute name `BuildIds` and its unversioned value `["unversioned"]`
    (SPEC-001 §3 R19b).

  No prompts, fixtures, schemas or test corpora were copied. The benchmark's
  histories in `temporal-port/bench/workloads.json` were written for this port.
- **Is behaviour derived even where no text was copied?** Yes, plainly and
  entirely. `ReplayEngine` is a deliberate reimplementation of
  `service/history/workflow/mutable_state_rebuilder.go`'s `ApplyEvents` and the
  `Apply*` methods of `mutable_state_impl.go` and `workflow_task_state_machine.go`
  that it dispatches to. Every rule in `SPEC-001 §3` cites the source line it
  reproduces and the run that established it. That derivation is the whole point of a
  port and is not something to be coy about.
- **What licence does that force on this project?** Nothing was copied, so nothing is
  forced. This project carries the MIT License, matching the source — chosen for
  compatibility rather than obligation, and reproducing the source's copyright notice
  is not required because no source text is included.

## Also used

- Akka (the Akka SDK for Java, 3.6.3) — the event-sourced entity, the HTTP endpoint
  and the test kit
- Jackson (via the Akka SDK) — the polymorphic event serialization that
  `temporal-port/probes/target-probe` established is required for a sealed interface
  inside a command
