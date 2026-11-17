package scheduler

import (
	"context"
	"encoding/json"
	"io"
	"strings"
	"time"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	re_builder "github.com/buildbarn/bb-remote-execution/pkg/builder"
	"github.com/prometheus/client_golang/prometheus"
)

// The task log is one line of JSON per change in the state of a task,
// from which the tasks that were queued, parked on a token pool or
// executing at any past moment can be found, with their invocations.
//
// Lines are numbered per scheduler process: order them by (Boot, Seq).
// A gap in Seq is a lost line. A restart logs "start" under a new Boot,
// which ends everything the previous process left open.

// Strings chosen by clients and workers are cut to this many bytes.
const taskLogMaximumStringSizeBytes = 1024

var inMemoryBuildQueueTaskLogLinesLostTotal = prometheus.NewCounter(
	prometheus.CounterOpts{
		Namespace: "buildbarn",
		Subsystem: "builder",
		Name:      "in_memory_build_queue_task_log_lines_lost_total",
		Help:      "Number of task log lines that were not written, either because the writer fell behind or because writing failed.",
	},
)

// TaskLogTokens is the number of tokens of a pool that a task needs.
type TaskLogTokens struct {
	Name   string `json:"name"`
	Amount uint32 `json:"amount"`
}

// TaskLogLine is a line of the task log. Seq, Boot, T and Ev are
// always set; the other fields depend on Ev. Fields that are zero are
// left out.
//
//   - "start": the scheduler process started.
//   - "queued": a task was queued, or queued again to be retried on
//     the largest size class. Describes the task.
//   - "attached": an Execute() call started waiting for a task under
//     a new operation. Names the invocation. Tasks the scheduler
//     queues by itself, to learn their size class, have none.
//   - "parked": a queued task started waiting for the token pool
//     Token, until its next "parked", "executing" or "finished" line.
//     During the startup grace period this is the first pool the task
//     needs, whether or not it has tokens left.
//   - "executing": a worker took the task, and the task its tokens.
//   - "finished": the attempt ended. Worker is absent if the task
//     never executed; otherwise the task returned its tokens.
//
// Lines of one call into the scheduler share T. An "executing" line may
// thus precede the "attached" line of the Execute() call that queued
// the task.
type TaskLogLine struct {
	Seq  uint64 `json:"seq"`
	Boot int64  `json:"boot"`
	T    string `json:"t"`
	Ev   string `json:"ev"`

	// Identifies a task within a Boot.
	Task uint64 `json:"task,omitempty"`

	// "queued". The tokens are of pools of InstanceNamePrefix.
	ActionHash         string `json:"action_hash,omitempty"`
	ActionSizeBytes    int64  `json:"action_size_bytes,omitempty"`
	InstanceName       string `json:"instance_name,omitempty"`
	InstanceNamePrefix string `json:"instance_name_prefix,omitempty"`
	TargetID           string `json:"target_id,omitempty"`

	// "queued" and "executing".
	SizeClass uint32          `json:"size_class,omitempty"`
	Tokens    []TaskLogTokens `json:"tokens,omitempty"`

	// "attached".
	Operation               string `json:"operation,omitempty"`
	Priority                int32  `json:"priority,omitempty"`
	ToolInvocationID        string `json:"tool_invocation_id,omitempty"`
	CorrelatedInvocationsID string `json:"correlated_invocations_id,omitempty"`
	ActionMnemonic          string `json:"action_mnemonic,omitempty"`

	// "parked".
	Token string `json:"token,omitempty"`

	// "executing" and "finished": the worker's ID, as a JSON object.
	Worker json.RawMessage `json:"worker,omitempty"`

	// "finished".
	CompletedByWorker bool   `json:"completed_by_worker,omitempty"`
	Result            string `json:"result,omitempty"`
	GRPCCode          string `json:"grpc_code,omitempty"`
	StatusMessage     string `json:"status_message,omitempty"`

	// "start".
	TokenPoolStartupGracePeriodSeconds float64 `json:"token_pool_startup_grace_period_seconds,omitempty"`
}

type taskLogState struct {
	boot       int64
	lastSeq    uint64
	lastTaskID uint64
}

// observeTaskLogLine and checkTaskLog are set by an internal test, so
// that every scheduler test replays the task log, and compares the
// result against the build queue every time its lock is released.
var (
	observeTaskLogLine func(bq *InMemoryBuildQueue, line *TaskLogLine)
	checkTaskLog       func(bq *InMemoryBuildQueue)
)

func truncateForTaskLog(s string) string {
	if len(s) > taskLogMaximumStringSizeBytes {
		// Cloned, so that a buffered line does not keep the whole
		// string alive.
		return strings.Clone(s[:taskLogMaximumStringSizeBytes])
	}
	return s
}

// logTask is called with the lock held, so the sink must not block.
func (bq *InMemoryBuildQueue) logTask(now time.Time, line *TaskLogLine) {
	bq.taskLog.lastSeq++
	line.Seq = bq.taskLog.lastSeq
	line.Boot = bq.taskLog.boot
	line.T = now.UTC().Format("2006-01-02T15:04:05.000000Z")
	if observeTaskLogLine != nil {
		observeTaskLogLine(bq, line)
	}
	bq.configuration.TaskLog(line)
}

func (bq *InMemoryBuildQueue) startTaskLog(now time.Time) {
	if bq.configuration.TaskLog == nil {
		return
	}
	bq.taskLog.boot = now.UnixMilli()
	bq.logTask(now, &TaskLogLine{
		Ev:                                 "start",
		TokenPoolStartupGracePeriodSeconds: bq.configuration.TokenPoolStartupGracePeriod.Seconds(),
	})
}

func (t *task) getTaskLogTokens() []TaskLogTokens {
	tokens := make([]TaskLogTokens, 0, len(t.tokenRequirements))
	for _, r := range t.tokenRequirements {
		tokens = append(tokens, TaskLogTokens{
			Name:   r.pool.key.name,
			Amount: r.amount,
		})
	}
	return tokens
}

func (bq *InMemoryBuildQueue) logTaskQueued(t *task) {
	if bq.configuration.TaskLog == nil {
		return
	}
	if t.logID == 0 {
		bq.taskLog.lastTaskID++
		t.logID = bq.taskLog.lastTaskID
	}
	scq := t.getCurrentSizeClassQueue()
	bq.logTask(bq.now, &TaskLogLine{
		Ev:                 "queued",
		Task:               t.logID,
		ActionHash:         t.desiredState.ActionDigest.GetHash(),
		ActionSizeBytes:    t.desiredState.ActionDigest.GetSizeBytes(),
		InstanceName:       truncateForTaskLog(t.actionDigest.GetInstanceName().String()),
		InstanceNamePrefix: scq.platformQueue.platformKey.GetInstanceNamePrefix().String(),
		TargetID:           truncateForTaskLog(t.targetID),
		SizeClass:          scq.sizeClass,
		Tokens:             t.getTaskLogTokens(),
	})
}

func (bq *InMemoryBuildQueue) logTaskAttached(o *operation, requestMetadata *remoteexecution.RequestMetadata) {
	if bq.configuration.TaskLog == nil {
		return
	}
	bq.logTask(bq.now, &TaskLogLine{
		Ev:                      "attached",
		Task:                    o.task.logID,
		Operation:               o.name,
		Priority:                o.priority,
		ToolInvocationID:        truncateForTaskLog(requestMetadata.GetToolInvocationId()),
		CorrelatedInvocationsID: truncateForTaskLog(requestMetadata.GetCorrelatedInvocationsId()),
		ActionMnemonic:          truncateForTaskLog(requestMetadata.GetActionMnemonic()),
	})
}

func (bq *InMemoryBuildQueue) logTaskParked(t *task) {
	if bq.configuration.TaskLog == nil {
		return
	}
	bq.logTask(bq.now, &TaskLogLine{
		Ev:    "parked",
		Task:  t.logID,
		Token: t.blockedPool.key.name,
	})
}

func (bq *InMemoryBuildQueue) logTaskExecuting(t *task, w *worker) {
	// task.complete() runs queued tasks through a temporary worker
	// that has no ID. Such tasks never execute.
	if bq.configuration.TaskLog == nil || w.workerKey == "" {
		return
	}
	bq.logTask(bq.now, &TaskLogLine{
		Ev:        "executing",
		Task:      t.logID,
		SizeClass: t.getCurrentSizeClassQueue().sizeClass,
		Tokens:    t.getTaskLogTokens(),
		Worker:    json.RawMessage(w.workerKey),
	})
}

// logTaskFinished must run while the task still has its worker and
// tokens, so its line precedes that of the task that takes them.
func (bq *InMemoryBuildQueue) logTaskFinished(t *task, executeResponse *remoteexecution.ExecuteResponse, completedByWorker bool) {
	if bq.configuration.TaskLog == nil {
		return
	}
	result, grpcCode := re_builder.GetResultAndGRPCCodeFromExecuteResponse(executeResponse)
	line := &TaskLogLine{
		Ev:                "finished",
		Task:              t.logID,
		CompletedByWorker: completedByWorker,
		Result:            result,
		GRPCCode:          grpcCode,
		StatusMessage:     truncateForTaskLog(executeResponse.GetStatus().GetMessage()),
	}
	if workerKey := t.currentWorker.workerKey; workerKey != "" {
		line.Worker = json.RawMessage(workerKey)
	}
	bq.logTask(bq.now, line)
}

// TaskLogWriter writes lines from a goroutine of its own, so that
// InMemoryBuildQueue never waits for the sink. Lines that don't fit in
// its buffer are dropped and counted.
type TaskLogWriter struct {
	w     io.Writer
	lines chan *TaskLogLine
}

// NewTaskLogWriter creates a TaskLogWriter that buffers at most
// bufferedLines lines. Run() writes them.
func NewTaskLogWriter(w io.Writer, bufferedLines int) *TaskLogWriter {
	return &TaskLogWriter{
		w:     w,
		lines: make(chan *TaskLogLine, bufferedLines),
	}
}

// Write queues a line without blocking.
func (lw *TaskLogWriter) Write(line *TaskLogLine) {
	select {
	case lw.lines <- line:
	default:
		inMemoryBuildQueueTaskLogLinesLostTotal.Inc()
	}
}

func (lw *TaskLogWriter) writeLine(line *TaskLogLine) {
	data, err := json.Marshal(line)
	if err == nil {
		_, err = lw.w.Write(append(data, '\n'))
	}
	if err != nil {
		inMemoryBuildQueueTaskLogLinesLostTotal.Inc()
	}
}

// Run writes lines until the context is done, and then the lines that
// are still buffered. It does not return while the sink blocks.
func (lw *TaskLogWriter) Run(ctx context.Context) {
	for {
		select {
		case line := <-lw.lines:
			lw.writeLine(line)
		case <-ctx.Done():
			for {
				select {
				case line := <-lw.lines:
					lw.writeLine(line)
				default:
					return
				}
			}
		}
	}
}
