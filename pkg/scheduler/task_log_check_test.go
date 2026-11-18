package scheduler

import (
	"encoding/json"
	"fmt"
	"slices"
	"sync"

	scheduler_invocation "github.com/buildbarn/bb-remote-execution/pkg/scheduler/invocation"
)

// Replay the task log of every build queue that the tests of this
// package create, and compare the result against the build queue every
// time its lock is released. A change of state that the log misses,
// misorders or misdescribes thus fails whichever test performs it.
func init() {
	observeTaskLogLine = func(bq *InMemoryBuildQueue, line *TaskLogLine) {
		r, _ := taskLogReplays.LoadOrStore(bq, &taskLogReplay{
			tasks: map[uint64]*taskLogReplayTask{},
			inUse: map[string]uint32{},
		})
		r.(*taskLogReplay).observe(bq, line)
	}
	checkTaskLog = func(bq *InMemoryBuildQueue) {
		if r, ok := taskLogReplays.Load(bq); ok {
			r.(*taskLogReplay).check(bq)
		} else if bq.configuration.TaskLog != nil {
			panic("Task log is on, but has no lines")
		}
	}
}

// Keyed by build queue. A replay is only used with the lock of its
// build queue held.
var taskLogReplays sync.Map

const (
	taskLogStageQueued    = "queued"
	taskLogStageParked    = "parked"
	taskLogStageExecuting = "executing"
	taskLogStageFinished  = "finished"
)

type taskLogReplayTask struct {
	stage              string
	instanceNamePrefix string
	sizeClass          uint32
	tokens             []TaskLogTokens
	// Set if stage is taskLogStageParked.
	pool string
	// Set if the current or last attempt was given to a worker.
	worker string
	// Set if a worker completed the last attempt.
	completedByWorker bool
	// The priority of every operation.
	operations map[string]int32
}

type taskLogReplay struct {
	lastSeq uint64
	tasks   map[uint64]*taskLogReplayTask
	// The number of tasks that are not taskLogStageFinished.
	unfinished int
	// Tokens held by executing tasks, by taskLogPoolKey().
	inUse map[string]uint32
}

func taskLogPoolKey(instanceNamePrefix, name string) string {
	return instanceNamePrefix + "\x00" + name
}

func (r *taskLogReplay) observe(bq *InMemoryBuildQueue, line *TaskLogLine) {
	fail := func(format string, args ...any) {
		panic(fmt.Sprintf("Task log line %d (%s of task %d): %s", line.Seq, line.Ev, line.Task, fmt.Sprintf(format, args...)))
	}
	if _, err := json.Marshal(line); err != nil {
		fail("cannot be written: %s", err)
	}
	if line.Seq != r.lastSeq+1 {
		fail("follows line %d", r.lastSeq)
	}
	r.lastSeq = line.Seq
	for _, s := range []string{line.InstanceName, line.TargetID, line.ToolInvocationID, line.CorrelatedInvocationsID, line.ActionMnemonic, line.StatusMessage} {
		if len(s) > taskLogMaximumStringSizeBytes {
			fail("has a string of %d bytes", len(s))
		}
	}
	if line.Boot != bq.taskLog.boot || line.T == "" {
		fail("has boot %d and time %#v", line.Boot, line.T)
	}

	if line.Ev == "start" {
		if line.Seq != 1 {
			fail("is not the first line")
		}
		return
	}
	t, ok := r.tasks[line.Task]
	if line.Ev == "queued" {
		if !ok {
			if line.Task != uint64(len(r.tasks))+1 {
				fail("is not the next new task")
			}
			t = &taskLogReplayTask{
				instanceNamePrefix: line.InstanceNamePrefix,
				tokens:             line.Tokens,
				operations:         map[string]int32{},
			}
			r.tasks[line.Task] = t
		} else if t.stage != taskLogStageFinished || !t.completedByWorker {
			// Only attempts that a worker reported as failed
			// are retried.
			fail("task is %s, completed by worker: %t", t.stage, t.completedByWorker)
		} else if t.instanceNamePrefix != line.InstanceNamePrefix || !slices.Equal(t.tokens, line.Tokens) {
			fail("retry needs other tokens")
		}
		t.stage, t.worker, t.sizeClass = taskLogStageQueued, "", line.SizeClass
		r.unfinished++
		return
	}
	if !ok {
		fail("task was never queued")
	}
	if t.stage == taskLogStageFinished {
		fail("task is finished")
	}
	switch line.Ev {
	case "attached":
		if _, ok := t.operations[line.Operation]; ok || line.Operation == "" {
			fail("operation %#v is not new", line.Operation)
		}
		t.operations[line.Operation] = line.Priority
	case "parked":
		if t.stage == taskLogStageExecuting {
			fail("task is executing")
		}
		if !slices.ContainsFunc(t.tokens, func(tokens TaskLogTokens) bool { return tokens.Name == line.Token }) {
			fail("task needs no tokens of pool %#v", line.Token)
		}
		t.stage, t.pool = taskLogStageParked, line.Token
	case "executing":
		if t.stage == taskLogStageExecuting {
			fail("task is executing")
		}
		if len(line.Worker) == 0 || line.SizeClass != t.sizeClass || !slices.Equal(t.tokens, line.Tokens) {
			fail("has worker %#v, size class %d and tokens %v", string(line.Worker), line.SizeClass, line.Tokens)
		}
		t.stage, t.pool, t.worker = taskLogStageExecuting, "", string(line.Worker)
		for _, tokens := range t.tokens {
			// Fails if the line of the task that returned
			// these tokens does not precede this one.
			key := taskLogPoolKey(t.instanceNamePrefix, tokens.Name)
			r.inUse[key] += tokens.Amount
			for poolKey, p := range bq.tokenPools {
				if taskLogPoolKey(poolKey.instanceNamePrefix.String(), poolKey.name) == key && r.inUse[key] > p.capacity {
					fail("%d tokens of pool %#v are held, of %d", r.inUse[key], tokens.Name, p.capacity)
				}
			}
		}
	case "finished":
		if string(line.Worker) != t.worker || (t.worker != "") != (t.stage == taskLogStageExecuting) {
			fail("has worker %#v, but task is %s on worker %#v", string(line.Worker), t.stage, t.worker)
		}
		if line.Result == "" || (line.CompletedByWorker && t.worker == "") {
			fail("has result %#v, completed by worker: %t", line.Result, line.CompletedByWorker)
		}
		if t.stage == taskLogStageExecuting {
			for _, tokens := range t.tokens {
				r.inUse[taskLogPoolKey(t.instanceNamePrefix, tokens.Name)] -= tokens.Amount
			}
		}
		t.stage, t.pool, t.completedByWorker = taskLogStageFinished, "", line.CompletedByWorker
		r.unfinished--
	default:
		fail("unknown event")
	}
}

func (r *taskLogReplay) check(bq *InMemoryBuildQueue) {
	if bq.taskLog.lastSeq != r.lastSeq {
		panic(fmt.Sprintf("Task log has %d lines, of which %d were observed", bq.taskLog.lastSeq, r.lastSeq))
	}

	unfinished := map[uint64]struct{}{}
	for _, o := range bq.operationsNameMap {
		t := o.task
		fail := func(format string, args ...any) {
			panic(fmt.Sprintf("Task %d of operation %s: %s", t.logID, o.name, fmt.Sprintf(format, args...)))
		}
		rt, ok := r.tasks[t.logID]
		if !ok {
			fail("is not in the task log")
		}
		if priority, ok := rt.operations[o.name]; !ok {
			if !slices.Equal(o.invocation.invocationKeys, scheduler_invocation.BackgroundLearningKeys) {
				fail("operation is not in the task log")
			}
		} else if priority != o.priority {
			fail("operation has priority %d, but %d according to the task log", o.priority, priority)
		}

		stage, pool, worker := taskLogStageQueued, "", ""
		switch {
		case t.executeResponse != nil:
			stage = taskLogStageFinished
		case t.currentWorker != nil:
			stage, worker = taskLogStageExecuting, string(t.currentWorker.workerKey)
		case t.blockedPool != nil:
			stage, pool = taskLogStageParked, t.blockedPool.key.name
		}
		if stage != rt.stage || pool != rt.pool || (stage == taskLogStageExecuting && worker != rt.worker) {
			fail("is %s on pool %#v and worker %#v, but %s on pool %#v and worker %#v according to the task log", stage, pool, worker, rt.stage, rt.pool, rt.worker)
		}
		if stage != taskLogStageFinished {
			unfinished[t.logID] = struct{}{}
			if sizeClass := t.getCurrentSizeClassQueue().sizeClass; sizeClass != rt.sizeClass {
				fail("is of size class %d, but %d according to the task log", sizeClass, rt.sizeClass)
			}
		}
	}
	if len(unfinished) != r.unfinished {
		panic(fmt.Sprintf("%d tasks are not finished, but %d according to the task log", len(unfinished), r.unfinished))
	}

	for poolKey, p := range bq.tokenPools {
		if inUse := r.inUse[taskLogPoolKey(poolKey.instanceNamePrefix.String(), poolKey.name)]; inUse != p.inUse {
			panic(fmt.Sprintf("Token pool %#v has %d tokens in use, but %d according to the task log", poolKey.name, p.inUse, inUse))
		}
	}
}
