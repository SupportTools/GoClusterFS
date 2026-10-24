# Multi-Task Execution Workflow - GoClusterFS

**Core Framework Document** - Workflow for executing multiple related tasks continuously (feature or sprint).

## When to Use This Workflow

Use this workflow when you want to:
- Complete all tasks in a **feature** (development phase) as a batch
- Complete all tasks in a **sprint** as a batch
- Execute a **custom list** of related tasks continuously

**For single tasks**, use [Task Execution Workflow](./task-execution-workflow.md) instead.

---

## Project Overview

GoClusterFS is a **single repository** FUSE-based clustered filesystem:
- **Language**: Go 1.21+
- **Architecture**: FUSE handlers + Block I/O + Journaling + Node Registry
- **TaskForge Project ID**: 88

---

## Workflow Summary

| Phase | Description | Gate |
|-------|-------------|------|
| 1. Batch Selection | Select tasks by feature/sprint/custom list | - |
| 2. Batch Planning | Research ALL tasks, present consolidated plan | - |
| 3. Batch Approval | Single approval for entire batch | **BLOCKER** |
| 4. Execution Loop | Per-task: Implement → Build → Test → Verify | **Per-task gates** |
| 5. Failure Handling | Stop, fix, resume | - |
| 6. Batch Completion | Summary, commits, follow-up tasks | - |

---

## Phase 1: Batch Selection (MANDATORY)

### Initiation Options

```
Question: "How would you like to select tasks for batch execution?"
Options:
- "By Feature" - All pending tasks in a development phase
- "By Sprint" - All pending tasks in a sprint
- "Custom List" - Specify task IDs manually
```

### Feature Selection

```javascript
// Get available features
mcp__taskforge__listFeatures({ projectId: 88 })

// Feature IDs for GoClusterFS:
// 444: Phase 1 - Core Infrastructure
// 445: Phase 2 - Basic Filesystem
// 446: Phase 3 - Journaling
// 447: Phase 4 - Cache Coherency
// 448: Phase 5 - Thin Provisioning
// 449: Phase 6 - Sub-Block Allocation
// 450: Phase 7 - Snapshots
// 451: Phase 8 - Production Hardening

// After user selects feature, query pending tasks
mcp__taskforge__getTasks({
  projectId: 88,
  featureId: <selected_feature_id>,
  status: "todo"
})
```

### Dependency Validation

After retrieving tasks:

1. **Build Dependency Graph** - Extract `blockedByTaskId` from each task
2. **Detect Cycles** - Fail fast if circular dependencies exist
3. **Topological Sort** - Determine execution order
4. **Validate Dependencies** - Ensure blocking tasks are completed or in batch

**Present Dependency Graph to User**:
```
Task A (ID: 4700) - no dependencies
  └── Task B (ID: 4701) - blocked by Task A
       └── Task C (ID: 4702) - blocked by Task B
Task D (ID: 4703) - no dependencies (parallel with A)
```

### TodoWrite: Initialize Batch Tracking

```
todos:
  - content: "Initialize batch (Feature: [name])"
    activeForm: "Initializing batch"
    status: "in_progress"
  - content: "Plan all X tasks in batch"
    activeForm: "Planning batch tasks"
    status: "pending"
  - content: "Obtain batch approval"
    activeForm: "Awaiting batch approval"
    status: "pending"
  - content: "Execute Task [ID]: [name]"  // One per task
    activeForm: "Executing Task [ID]"
    status: "pending"
```

---

## Phase 2: Batch Planning (MANDATORY)

**No coding until this phase is complete.**

### Research All Tasks

For EACH task in the batch:
1. Review CLAUDE.md for architecture specifications
2. Identify affected packages and files
3. Review existing codebase for patterns
4. Understand filesystem integrity implications

### Consolidated Plan Format

Present a single plan covering ALL tasks:

```markdown
## Batch Execution Plan: [Feature Name]

**Scope**: X tasks | Z packages affected

### Dependency Graph
[ASCII representation of task dependencies]

### Execution Order
1. Task [ID]: [Name] (no dependencies)
2. Task [ID]: [Name] (after Task 1)
...

---

### Task 1: [Task Name] (ID: XXXX)

**Priority**: High

**Packages Affected**:
- `internal/block/` - Block device operations
- `internal/journal/` - WAL implementation

**Implementation Summary**:
Brief description of the implementation approach...

**Success Criteria**:
- [ ] Build passes (`go build`)
- [ ] Tests pass (`go test ./...`)
- [ ] No data corruption paths introduced

**Dependencies**: None

---

[... for each task ...]

---

### Risks & Mitigations

| Risk | Mitigation |
|------|------------|
| Data corruption | Validate all block indices |
| Race conditions | Use proper synchronization |
```

---

## Phase 3: Batch Approval Gate (MANDATORY)

**Single approval for entire batch. Do not proceed without explicit approval.**

```
Question: "Batch execution plan ready for [Feature Name] with X tasks."
Options:
- "Approve and begin execution" - Start continuous execution
- "Request changes to plan" - Provide specific feedback
- "Remove tasks from batch" - Exclude specific tasks
- "Abort batch" - Cancel batch execution
```

---

## Phase 4: Batch Execution Loop

Execute tasks in dependency order. Each task goes through **full quality gates**.

### Per-Task Execution Flow

```
┌─────────────────────────────────────────┐
│ For each task in execution order:       │
├─────────────────────────────────────────┤
│ 4.1 Update TaskForge: status=in_progress│
│ 4.2 Implementation                      │
│ 4.3 Build Verification (BLOCKER)        │
│ 4.4 Format Check (BLOCKER)              │
│ 4.5 Test Suite (BLOCKER)                │
│ 4.6 FUSE Verification (conditional)     │
│ 4.7 Task Completion                     │
│     └── Update TaskForge: status=done   │
│     └── Git commit                      │
│     └── Move to next task OR complete   │
└─────────────────────────────────────────┘
```

### 4.1 Start Task

```javascript
mcp__taskforge__updateTask({
  taskId: <current_task_id>,
  status: "in_progress"
})
```

### 4.2 Implementation

Follow implementation guidelines from [Task Execution Workflow](./task-execution-workflow.md):
- Go formatting (`gofmt`)
- Comprehensive error handling
- Thread-safe operations
- Write tests alongside code

### 4.3 Build Verification (BLOCKER)

```bash
make build
```

**MUST PASS** - Zero errors. If fails, fix and retry.

### 4.4 Format Check (BLOCKER)

```bash
gofmt -l .
```

Must have no output (all files properly formatted).

### 4.5 Test Suite (BLOCKER)

```bash
go test ./... -v
```

All tests must pass. If fails, fix → re-build → re-test.

### 4.6 FUSE Verification (Conditional)

When task affects filesystem operations:

```bash
# Create test environment
truncate -s 100M test.img
mkdir -p testmount

# Mount and test
./goclusterfs-mount -device ./test.img -mount ./testmount -format > mount.log 2>&1 &
sleep 2

# Verify operations
ls -la testmount
echo "test" > testmount/testfile
cat testmount/testfile

# Unmount
fusermount3 -u testmount
```

### 4.7 Task Completion

```javascript
mcp__taskforge__updateTask({
  taskId: <current_task_id>,
  status: "done",
  outcome: "Summary of implementation"
})
```

**Git commit:**
```bash
git add .
git commit -m "<type>: task description

Task #XXXX: [task name]
- Key changes

Co-Authored-By: Claude <noreply@anthropic.com>"
```

**TodoWrite Update:**
```
Mark current "Execute Task [ID]" as completed
Mark next "Execute Task [ID]" as in_progress (if more tasks)
```

**Continue to next task or proceed to Phase 6 (Batch Completion).**

---

## Phase 5: Failure Handling

### On Any Gate Failure

1. **STOP** batch execution immediately
2. **Present** options to user:

```
Question: "Task [name] failed at [gate]. How would you like to proceed?"
Options:
- "Fix applied, retry current task" - Re-enter from build verification
- "Skip this task, continue batch" - Mark task as blocked, move to next
- "Abort batch, keep completed work" - End batch, completed tasks remain done
```

### Resume Semantics

**Fix and Retry:**
1. Make fixes to code
2. Resume from **Build Verification** (4.3)
3. Continue through remaining gates

**Skip Task:**
```javascript
mcp__taskforge__updateTask({
  taskId: <failed_task_id>,
  status: "blocked",
  outcome: "Skipped during batch execution: [reason]"
})
```
- Check if any remaining tasks depend on skipped task
- Continue with next independent task

---

## Phase 6: Batch Completion

### Batch Completion Summary

```markdown
## Batch Completion Summary: [Feature Name]

**Tasks**: Y completed / Z total (A skipped)

### Completed Tasks

| ID | Task Name | Gates |
|----|-----------|-------|
| 4700 | Block bitmap implementation | All passed |
| 4701 | Inode table structure | All passed |

### Testing Results

- **Unit tests**: X passing, 0 failing
- **Coverage**: Y%

### Git Commits

- `abc1234` - feat(block): add block bitmap implementation (#4700)
- `def5678` - feat(inode): add inode table structure (#4701)

### Follow-up Tasks Created (if any)

| New ID | Origin Task | Finding | Priority |
|--------|-------------|---------|----------|
| 4750 | #4700 | Add concurrent access tests | Medium |
```

### TodoWrite Cleanup

```
Mark all "Execute Task [ID]" as completed
Clear todo list for next batch
```

---

## State Management

### Dual Tracking System

| System | Scope | Persistence | Purpose |
|--------|-------|-------------|---------|
| **TodoWrite** | Current session | Session-only | Visual progress tracking |
| **TaskForge** | Cross-session | Permanent | Task state across sessions |

---

## Quality Gates Summary

| Phase | Gate | Command | Blocker |
|-------|------|---------|---------|
| Build | Binary compiles | `make build` | YES |
| Format | Code formatted | `gofmt -l .` | YES |
| Test | All tests pass | `go test ./... -v` | YES |
| FUSE | Mount/unmount works | Manual test | Conditional |

---

## Absolute Rules

### MANDATORY

1. **BATCH PLANNING FIRST** - Research ALL tasks before approval
2. **SINGLE APPROVAL GATE** - Get batch approval once, execute continuously
3. **PER-TASK QUALITY GATES** - Every task: Build → Format → Test
4. **DEPENDENCY ORDER** - Execute tasks in topological order
5. **NO DATA CORRUPTION** - Every task must preserve filesystem integrity

### FORBIDDEN

1. Skipping quality gates for any task
2. Ignoring dependencies in execution order
3. Modifying completed tasks after gates pass
4. Continuing with failing builds/tests
5. Writing to block device without proper error handling

---

## Workflow Diagram

```
┌─────────────────────────────────────────────────────────────┐
│ Phase 1: Batch Selection                                    │
│    - Select by Feature/Sprint/Custom list                   │
│    - Build dependency graph                                 │
└────────────────────────┬────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────┐
│ Phase 2: Batch Planning                                     │
│    - Research ALL tasks                                     │
│    - Present consolidated plan                              │
└────────────────────────┬────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────┐
│ Phase 3: Batch Approval (GATE)                              │
│    ✅ Approve → Continue                                     │
│    🔄 Revise → Back to Phase 2                              │
│    ❌ Abort → End workflow                                   │
└────────────────────────┬────────────────────────────────────┘
                         │ (approved)
                         ▼
┌─────────────────────────────────────────────────────────────┐
│ Phase 4: Execution Loop (per task)                          │
│    4.1 Start task (TaskForge: in_progress)                  │
│    4.2 Implementation                                       │
│    4.3 Build Verification (BLOCKER)                         │
│    4.4 Format Check (BLOCKER)                               │
│    4.5 Test Suite (BLOCKER)                                 │
│    4.6 FUSE Verification (conditional)                      │
│    4.7 Task Completion                                      │
│         │                                                   │
│    ┌────┴─────────────────────────────┐                     │
│    │ More tasks? → YES → Next task    │                     │
│    │              → NO  → Phase 6     │                     │
│    └──────────────────────────────────┘                     │
└────────────────────────┬────────────────────────────────────┘
                         │
           ┌─────────────┴─────────────┐
           │                           │
           ▼                           ▼
┌──────────────────────┐    ┌─────────────────────────────────┐
│ Phase 5: On Failure  │    │ Phase 6: Batch Completion       │
│  - Stop execution    │    │    - Provide summary            │
│  - Present options   │    │    - All tasks marked done      │
│  - Fix/Skip/Abort    │    │    - Await next batch           │
└──────────────────────┘    └─────────────────────────────────┘
```

---

This workflow ensures systematic, quality-driven batch execution for GoClusterFS development while maintaining filesystem integrity and code quality.

See also:
- [Task Execution Workflow](./task-execution-workflow.md) for single-task execution
