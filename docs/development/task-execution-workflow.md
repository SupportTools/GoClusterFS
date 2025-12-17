# Task Execution Workflow for GoClusterFS

This document defines the mandatory workflow for executing all development tasks in the GoClusterFS project.

## Available Resources

### Task Management System

The project uses PostgreSQL-backed TaskForge for persistent task management:
- **TaskForge Project ID: 88**
- **Priority-based**: Urgent, High, Medium, Low
- **Persistent across sessions**: All Claude Code instances share the same task state
- **MCP Tools**: `mcp__taskforge__getTasks`, `mcp__taskforge__updateTask`, `mcp__taskforge__getTask`

### Development Environment

**Requirements:**
- Go 1.21+ (check with `go version`)
- FUSE 3 libraries (`libfuse3-dev` on Debian/Ubuntu)
- Linux kernel with FUSE support

**Block Device Setup:**
```bash
# Create a test block device (3GB)
dd if=/dev/zero of=VirtBlock.img bs=1M count=3000

# Or use a loopback device for testing
sudo losetup -f VirtBlock.img
```

### Repository Structure

GoClusterFS is a **single repository** project:

```
GoClusterFS/
├── main.go              # FUSE mount entry point
├── internal/
│   ├── block/           # Block device I/O, O_DIRECT
│   ├── journal/         # Write-ahead logging
│   ├── node/            # Cluster node registry
│   └── fs/              # FUSE filesystem handlers
├── docs/                # Documentation
└── CLAUDE.md            # Project coding standards
```

---

## Workflow Steps

### 1. Receive and Initiate Task

- Query TaskForge for task: `mcp__taskforge__getTask({taskId: <id>})`
- Verify task priority and check for dependencies (`blockedByTaskId`)
- If task has unmet dependencies, report and await instruction
- Update task status to `in_progress`: `mcp__taskforge__updateTask({taskId: <id>, status: "in_progress"})`

**TodoWrite Integration (MANDATORY):**
```
Use TodoWrite tool to create task breakdown:
- Research and design phase
- Implementation phase
- Build verification
- Testing
- Task completion

Mark first item as "in_progress" immediately.

NOTE: TodoWrite tracks workflow steps within current session.
TaskForge tracks task completion across all sessions.
```

### 2. Research & Design Phase (MANDATORY - NO CODING WITHOUT THIS)

**Research the task requirements:**
- Review CLAUDE.md for architecture and coding standards
- Identify affected packages and files
- Review existing code patterns
- Understand impact on filesystem integrity and data safety

**Present a comprehensive implementation plan:**

#### Task Overview
Clear summary aligned with project architecture

#### Design Approach
Technical strategy following established patterns:
- Block-level operations (1MB blocks)
- Metadata handling (gob encoding in block 0)
- FUSE handler patterns
- Thread-safe concurrent access

#### Files Affected
Complete list with brief explanation

#### Dependencies
Prerequisites and blockers

#### Success Criteria
Specific, measurable conditions:
- Build passes (`go build`)
- All tests pass (`go test ./...`)
- FUSE mount/unmount works correctly
- No data corruption paths

#### Risks & Mitigations
Potential issues, especially:
- Data corruption risks
- Race conditions
- Block device safety

### 3. Wait for Approval (MANDATORY GATE)

**Pause here and await explicit instruction.** Do not proceed without approval.

- `Approve`: Proceed to Implementation
- `Request changes`: Revise plan and re-present
- `Skip`: Mark task as skipped and await new assignment

### 4. Implementation Phase

Implement the approved design with strict adherence to:

#### Coding Standards
- Go formatting (`gofmt`)
- Comprehensive error handling (filesystem operations must not lose data)
- Clear logging for debugging
- Thread-safe operations for concurrent FUSE access

#### Testing
- Write tests alongside code (MANDATORY)
- Test happy paths, edge cases, and error conditions
- Consider concurrent access scenarios

#### Filesystem Safety
- Validate all block indices before access
- Use O_DIRECT where appropriate for data integrity
- Implement proper sync/flush semantics
- Handle partial writes gracefully

**If implementation reveals issues with approved plan, stop and report immediately**

### 5. Quality Assurance Cycle (MANDATORY)

#### Step 5.1: Build Verification (BLOCKER)

```bash
go build -o GoClusterFS .
```

- **MUST PASS**: Zero compilation errors
- If build fails, fix immediately before proceeding

#### Step 5.2: Format Check (BLOCKER)

```bash
gofmt -l .
```

- Must have no output (all files properly formatted)
- Fix with: `gofmt -w .`

#### Step 5.3: Run Test Suite (BLOCKER)

```bash
go test ./... -v
```

- Report test coverage and results
- **If tests fail**: Fix issues, re-build, re-test
- Test results must include: pass/fail counts

#### Step 5.4: FUSE Verification (when applicable)

```bash
# Create test environment
dd if=/dev/zero of=/tmp/test.img bs=1M count=100
mkdir -p /tmp/testmount

# Mount and test
./GoClusterFS /tmp/testmount /tmp/test.img &
sleep 2

# Verify basic operations
ls -la /tmp/testmount
echo "test" > /tmp/testmount/testfile
cat /tmp/testmount/testfile

# Unmount
fusermount -u /tmp/testmount
```

### 6. Task Completion

#### Provide completion summary:
- Files changed/created/deleted
- Tests written with coverage metrics
- Any known limitations or follow-up work needed

#### Update TaskForge:
```
mcp__taskforge__updateTask({taskId: <id>, status: "done"})
```

#### Commit with proper format:
```bash
git add .
git commit -m "<type>: <description>

- Detailed changes
- Task: #<task_id>

Co-Authored-By: Claude <noreply@anthropic.com>"
```

Types: feat, fix, refactor, test, docs, chore

---

## GoClusterFS-Specific Requirements

### ABSOLUTE RULES

1. **ONE TASK AT A TIME** - Never work on multiple tasks simultaneously
2. **RESEARCH BEFORE CODING** - Mandatory research phase, no exceptions
3. **BUILD VERIFICATION** - Must pass before testing
4. **NO DATA CORRUPTION** - All changes must be safe for filesystem integrity
5. **THREAD SAFETY** - FUSE operations can be concurrent

### FORBIDDEN ACTIONS

- Starting new task before current fully completed
- Skipping workflow steps
- Proceeding with failed builds
- Writing to block device without proper error handling
- Ignoring race condition possibilities
- Modifying metadata without proper serialization

### Data Integrity Checklist

Before marking task complete, verify:
- [ ] All block writes are followed by appropriate sync
- [ ] Error paths don't leave filesystem in inconsistent state
- [ ] Metadata updates are atomic or journaled
- [ ] Concurrent access is properly synchronized
- [ ] File operations handle interrupts gracefully

---

## Quality Gates Summary

| Phase | Gate | Blocker | Tool |
|-------|------|---------|------|
| Research | Design approved | Yes | Manual approval |
| Build | Binary compiles | Yes | `go build` |
| Format | Code formatted | Yes | `gofmt -l .` |
| Test | All tests pass | Yes | `go test ./...` |
| FUSE | Mount/unmount works | Yes | Manual test |

---

## Workflow Diagram

```
┌─────────────────────────────────────────────────────────────┐
│ 1. Receive Task                                             │
│    - Verify priority & dependencies                         │
│    - Mark [IN-PROGRESS] in TaskForge                        │
└────────────────────────┬────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────┐
│ 2. Research & Design Phase (MANDATORY)                      │
│    - Review CLAUDE.md                                       │
│    - Identify affected files                                │
│    - Present implementation plan                            │
└────────────────────────┬────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────┐
│ 3. Wait for Approval (GATE)                                 │
│    ✅ Approve → Continue                                     │
│    🔄 Revise → Back to Step 2                               │
│    ⏭️ Skip → Await new task                                 │
└────────────────────────┬────────────────────────────────────┘
                         │ (approved)
                         ▼
┌─────────────────────────────────────────────────────────────┐
│ 4. Implementation Phase                                     │
│    - Follow coding standards                                │
│    - Write tests alongside code                             │
│    - Ensure thread safety                                   │
└────────────────────────┬────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────┐
│ 5.1 Build Verification (BLOCKER)                            │
│     go build -o GoClusterFS .                               │
│     ❌ Failed → Fix → Retry                                  │
└────────────────────────┬────────────────────────────────────┘
                         │ (passed)
                         ▼
┌─────────────────────────────────────────────────────────────┐
│ 5.2 Format Check (BLOCKER)                                  │
│     gofmt -l .                                              │
│     ❌ Issues → Fix → Retry                                  │
└────────────────────────┬────────────────────────────────────┘
                         │ (passed)
                         ▼
┌─────────────────────────────────────────────────────────────┐
│ 5.3 Run Test Suite (BLOCKER)                                │
│     go test ./... -v                                        │
│     ❌ Failed → Fix → Back to 5.1                            │
└────────────────────────┬────────────────────────────────────┘
                         │ (passed)
                         ▼
┌─────────────────────────────────────────────────────────────┐
│ 5.4 FUSE Verification (when applicable)                     │
│     Mount, test operations, unmount                         │
│     ❌ Failed → Fix → Back to 5.1                            │
└────────────────────────┬────────────────────────────────────┘
                         │ (passed)
                         ▼
┌─────────────────────────────────────────────────────────────┐
│ 6. Task Completion                                          │
│    - Provide summary                                        │
│    - Mark [DONE] in TaskForge                               │
│    - Commit changes                                         │
│    - Await next task                                        │
└─────────────────────────────────────────────────────────────┘
```

---

This workflow ensures systematic, quality-driven development aligned with GoClusterFS architecture and filesystem safety requirements.
