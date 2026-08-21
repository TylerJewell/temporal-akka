# temporal-akka

Reads a workflow's recorded history back and works out what state the workflow was in.

A port of [temporalio/temporal](https://github.com/temporalio/temporal) onto **Akka**, built
with **Akka Specify**.

---

## Where it came from

temporalio/temporal is a server that runs long-lived programs, keeping a permanent record of
everything that happened to each one so it can be picked up again after a crash, a restart or
a move to another machine. One piece of that server reads such a record back and works out
where the program had got to. That piece is what was ported, to derive a specification format
precise enough to regenerate a system on a different stack — the port is the vehicle, the
specification is the deliverable.

The specifications the port was generated from are in
[TylerJewell/akka-specify-harness](https://github.com/TylerJewell/akka-specify-harness) under
`temporal-port/`.

---

## temporalio/temporal → this port

📉 1,654 Go lines → **673 Java lines**<br>
📁 6 files → **6 files**<br>
⚡ 159.8 → **7.6** microseconds to read back a 300-entry record<br>
🎯 13 of 13 comparisons agreeing → **13 of 13**<br>
🧾 18 of 18 checks failing when the rule they name is broken → **18 of 18**

Full method and the numbers that did *not* make this list:
[`bench/REPORT.md`](https://github.com/TylerJewell/akka-specify-harness/blob/main/temporal-port/bench/REPORT.md).

---

## What it took to build

⏱️ **1.9 hours** from the first command to the published repository, **1.9** of them active<br>
💬 **488** exchanges with the model<br>
✍️ **531,503** tokens written by the model, **182,540,347** counting everything sent and re-sent<br>
🙋 **0** questions to a human<br>
🧪 **43** tests

```bash
python toolkit/tokens.py --port temporal    # turns, tokens, elapsed and active time
```

The record of every question, and where the time went, is in
[`port-log/`](https://github.com/TylerJewell/akka-specify-harness/tree/main/port-log).

---

## What it does

A workflow's record arrives as a list of entries, in groups that were written together. Each
entry says one thing that happened: the workflow started, a piece of work was handed out, a
timer was set, a message arrived, the workflow finished. Reading the whole list in order
produces the workflow's state — what is still outstanding, what has been done, whether it is
still going.

From the specification:

- **A group of entries is read as a unit, and where the groups fall changes one part of the
  answer.** The same list cut into different groups produces the same state everywhere except
  one field, which records where the last group started.
- **The position counter is not readable while a group is being read.** Anything that asks
  for it mid-read gets the value from before the read began, and one piece of the answer is
  built out of that value.
- **An entry that mentions something not outstanding is passed over in silence, with three
  exceptions.** Fifteen kinds do nothing at all; the three that say a piece of work has begun
  refuse a record that never handed that work out.
- **A group whose last entry is older than the previous group's is refused.** That is the
  only thing the reader checks; gaps and repeats inside a group are read without complaint.
- **Reading one record twice gives the same answer twice.** Two identifiers that would
  otherwise be made up fresh each time are worked out from the record instead.

---

## Design decisions

**Event sourcing.** A workflow's record is already a list of things that happened, kept
forever and read back in order, which is exactly what this kind of storage is for. Storing
each entry as its own item means reading the record back is not something the program does —
it is what the storage does when it loads.

**Group markers.** Some of the rules are about a whole group of entries rather than any one
of them, and those rules would be lost if only the entries were kept. Writing a marker before
and after each group keeps them, so a state loaded from storage months later is the same
state the caller was shown at the time.

**Two ways in, one set of rules.** A record arriving for the first time is checked and may be
refused; a record being loaded back has already been checked, and refusing it then would
leave the workflow unloadable. Both go through the same rules, with the refusals turned off
for the second, and a test compares the two so they cannot drift apart.

**Worked-out identifiers.** The original makes up a fresh identifier every time it reads a
record, so reading one record twice gives two different answers. Working the identifier out
from the record instead means the answer depends only on what was recorded, which is what
lets the two systems be compared at all.

**One record per run.** Each attempt at a workflow gets its own storage item, keyed by the
workflow's name and the attempt's identifier together. Two attempts at the same workflow
never share a history, so one cannot be read as part of the other.

---

## Running it — the short path

You do not need Java, Maven, or the Akka CLI installed. Akka Specify installs them for you.

**1. Install Akka Specify** in Claude Code:

```
/plugin marketplace add akka/ai-marketplace
/plugin install akka@akka-ai-marketplace
```

Restart Claude Code when it asks.

**2. Give it this prompt:**

> Clone https://github.com/TylerJewell/temporal-akka into a new directory and open it.
> Then run /akka:setup to install everything this project needs, and /akka:build to
> compile it, run the tests, and start it locally.

**3. Open** http://localhost:9042.

---

## Running it — the developer path

### Requirements

- Java 21 or newer
- Maven 3.9 or newer
- An Akka download token — run `akka code token` once

### Start the service

```bash
mvn compile
akka local run
```

The service starts on **port 9042**.

### Read a record back

Hand over one group of entries at a time, then ask for the state:

```bash
curl -X POST http://localhost:9042/replay/order-42/run-1/batches \
  -H 'Content-Type: application/json' \
  -d '{"events":[
        {"eventId":1,"version":0,"taskId":10,
         "attributes":{"type":"WorkflowExecutionStarted","workflowType":"T",
                       "taskQueue":"tq","firstExecutionRunId":"run-1","attempt":1}},
        {"eventId":2,"version":0,"taskId":20,
         "attributes":{"type":"WorkflowTaskScheduled","taskQueue":"tq",
                       "startToCloseTimeoutMillis":60000,"attempt":1}}]}'

curl http://localhost:9042/replay/order-42/run-1
```

`POST /replay/direct/{runId}` reads a whole record without keeping it, and
`POST /replay/structure` reports the gaps and repeats the reader itself passes over.

---

## Configuration

| Variable | Default | Notes |
|---|---|---|
| `akka.javasdk.dev-mode.http-port` | `9042` | set in `src/main/resources/application.conf` |

No model provider is used: nothing here calls a language model.

---

## Where it differs from temporalio/temporal

Everything not listed here behaves the same way on purpose, including the parts that look
like mistakes.

- **Identifiers for cancelling and messaging another workflow.** temporalio/temporal makes up
  a fresh identifier each time it reads the record, so reading one record twice gives two
  different answers. This port works the identifier out from the workflow attempt and the
  entry that asked for it, so a second read gives the same answer as the first — without
  that, the two systems could not be compared on the field at all.
- **How much can be handed over at once.** temporalio/temporal reads a record straight out of
  its own storage, a page at a time, with no ceiling a caller can hit. This port takes the
  record over a network connection, and the runtime refuses anything past 1,048,485 bytes
  with a message naming the limit; groups of five thousand entries were accepted in testing.
  Records are handed over one group per call, which is also how the original's own caller
  does it.
- **What is kept about each entry.** The original carries the full contents of every entry —
  the inputs handed to a piece of work, the results that came back, the text of a failure.
  This port carries the identifiers, names and counts the answer is built from and nothing
  else, because none of the rest is read while working out the state.
- **Search labels.** Both record the same label names, and this port stores each value as
  plain text where the original stores an encoded package with its own type information. The
  one value in scope, the worker's version marker, is reproduced exactly; the rest is
  **not checked**.
- **Entry kinds outside the thirty-four in scope.** Live updates to a running workflow, calls
  out to other services through the newer bridge, pausing, and the newer component framework
  are not built. This port refuses them by name, which is what the original does with an
  entry kind it does not know — but that is not the same as agreeing about them, and it is
  **not checked**.
- **Versioned workers.** Every record tested was from an unversioned worker, which is the
  only case this port builds. Behaviour with a versioned one is **not checked**.
- **Two records arriving at once.** The original serialises work on one workflow through a
  lock on its shard; this port serialises through one storage item per attempt. Neither was
  tested with two groups arriving together, so this is **not checked**.
- **Everything the original does besides reading a record back.** Handing work to workers,
  timing things out, storing anything, answering the operator tools, and running across
  regions are not built here. The specification says what is in scope and why.

---

## Licence

temporalio/temporal is MIT, © 2025 Temporal Technologies Inc. and © 2020 Uber Technologies,
Inc. This port reimplements the behaviour without copied source; see `ACKNOWLEDGEMENTS.md`.
