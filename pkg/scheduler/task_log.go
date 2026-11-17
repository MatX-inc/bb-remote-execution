package scheduler

import (
	"context"
	"encoding/json"
	"io"
	"time"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	re_builder "github.com/buildbarn/bb-remote-execution/pkg/builder"
	"github.com/prometheus/client_golang/prometheus"
)

// The task log is one line of JSON per change in the state of a task,
// from which the queue at any past moment can be rebuilt.
//
// Lines are numbered per scheduler process: order them by (Boot, Seq).
// A gap in Seq is a lost line. A restart logs "start" under a new Boot,
// which ends everything the previous process left open.

const (
	taskLogHeartbeatInterval = 10 * time.Second
	// Strings chosen by clients are cut to this many bytes.
	taskLogMaximumStringSizeBytes = 1024
)

var inMemoryBuildQueueTaskLogLinesLostTotal = prometheus.NewCounter(
	prometheus.CounterOpts{
		Namespace: "buildbarn",
		Subsystem: "builder",
		Name:      "in_memory_build_queue_task_log_lines_lost_total",
		Help:      "Number of task log lines that were not written, either because the writer fell behind or because writing failed.",
	},
)

// TaskLogTokens is a task's demand on, or the state of, one token pool.
type TaskLogTokens struct {
	InstanceNamePrefix string `json:"instance_name_prefix,omitempty"`
	Name               string `json:"name"`
	Amount             uint32 `json:"amount,omitempty"`
	Capacity           uint32 `json:"capacity,omitempty"`
	InUse              uint32 `json:"in_use,omitempty"`
	Blocked            int    `json:"blocked,omitempty"`
}

// TaskLogLine is a line of the task log. Seq, Boot, T and Ev are
// always set; the other fields depend on Ev:
//
//   - "start": the scheduler process started.
//   - "heartbeat": written every ten seconds; carries the token pools.
//   - "queued": a task was queued, or queued again to be retried on
//     the largest size class. Describes the task.
//   - "attached": an Execute() call started waiting for a task. Names
//     the invocation.
//   - "parked": a queued task started waiting for the token pool
//     Token, until its next "executing" or "finished" line.
//   - "executing": a worker took the task, and the task its tokens.
//   - "finished": the attempt ended and returned its tokens. Worker is
//     absent if the task never executed.
//   - "detached": an operation was removed; others keep the task.
//   - "removed": the task's last operation was removed.
type TaskLogLine struct {
	Seq  uint64 `json:"seq"`
	Boot int64  `json:"boot"`
	T    string `json:"t"`
	Ev   string `json:"ev"`

	// Identifies a task within a Boot.
	Task      uint64 `json:"task,omitempty"`
	Operation string `json:"operation,omitempty"`

	// "queued".
	ActionHash         string `json:"action_hash,omitempty"`
	ActionSizeBytes    int64  `json:"action_size_bytes,omitempty"`
	DigestFunction     string `json:"digest_function,omitempty"`
	InstanceName       string `json:"instance_name,omitempty"`
	InstanceNamePrefix string `json:"instance_name_prefix,omitempty"`
	Platform           string `json:"platform,omitempty"`
	TargetID           string `json:"target_id,omitempty"`
	DoNotCache         bool   `json:"do_not_cache,omitempty"`
	BackgroundLearning bool   `json:"background_learning,omitempty"`

	// "queued", "executing" and "finished".
	SizeClass uint32 `json:"size_class,omitempty"`

	// "queued" and "executing".
	Tokens []TaskLogTokens `json:"tokens,omitempty"`

	// "attached".
	Priority                int32  `json:"priority,omitempty"`
	Deduplicated            bool   `json:"deduplicated,omitempty"`
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
	ExitCode          int32  `json:"exit_code,omitempty"`
	WorkerRetries     int    `json:"worker_retries,omitempty"`

	// "start".
	HeartbeatSeconds                   float64 `json:"heartbeat_seconds,omitempty"`
	TokenPoolStartupGracePeriodSeconds float64 `json:"token_pool_startup_grace_period_seconds,omitempty"`

	// "heartbeat".
	Tasks      uint64          `json:"tasks,omitempty"`
	TokenPools []TaskLogTokens `json:"token_pools,omitempty"`
}

type taskLogState struct {
	boot                int64
	lastSeq             uint64
	lastTaskID          uint64
	heartbeatCleanupKey cleanupKey
}

func truncateForTaskLog(s string) string {
	if len(s) > taskLogMaximumStringSizeBytes {
		return s[:taskLogMaximumStringSizeBytes]
	}
	return s
}

// logTask is called with the lock held, so the sink must not block.
func (bq *InMemoryBuildQueue) logTask(now time.Time, line *TaskLogLine) {
	bq.taskLog.lastSeq++
	line.Seq = bq.taskLog.lastSeq
	line.Boot = bq.taskLog.boot
	line.T = now.UTC().Format("2006-01-02T15:04:05.000000Z")
	bq.configuration.TaskLog(line)
}

func (bq *InMemoryBuildQueue) startTaskLog(now time.Time) {
	if bq.configuration.TaskLog == nil {
		return
	}
	bq.taskLog.boot = now.UnixMilli()
	bq.logTask(now, &TaskLogLine{
		Ev:                                 "start",
		HeartbeatSeconds:                   taskLogHeartbeatInterval.Seconds(),
		TokenPoolStartupGracePeriodSeconds: bq.configuration.TokenPoolStartupGracePeriod.Seconds(),
	})
	bq.cleanupQueue.add(&bq.taskLog.heartbeatCleanupKey, now.Add(taskLogHeartbeatInterval), bq.logTaskHeartbeat)
}

func (bq *InMemoryBuildQueue) logTaskHeartbeat() {
	pools := bq.sortedTokenPools()
	line := &TaskLogLine{
		Ev:         "heartbeat",
		Tasks:      bq.taskLog.lastTaskID,
		TokenPools: make([]TaskLogTokens, 0, len(pools)),
	}
	for _, p := range pools {
		line.TokenPools = append(line.TokenPools, TaskLogTokens{
			InstanceNamePrefix: p.key.instanceNamePrefix.String(),
			Name:               p.key.name,
			Capacity:           p.capacity,
			InUse:              p.inUse,
			Blocked:            len(p.blocked),
		})
	}
	bq.logTask(bq.now, line)
	bq.cleanupQueue.add(&bq.taskLog.heartbeatCleanupKey, bq.now.Add(taskLogHeartbeatInterval), bq.logTaskHeartbeat)
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
	platformKey := scq.platformQueue.platformKey
	line := &TaskLogLine{
		Ev:                 "queued",
		Task:               t.logID,
		ActionHash:         t.desiredState.ActionDigest.GetHash(),
		ActionSizeBytes:    t.desiredState.ActionDigest.GetSizeBytes(),
		DigestFunction:     t.desiredState.DigestFunction.String(),
		InstanceName:       truncateForTaskLog(t.actionDigest.GetInstanceName().String()),
		InstanceNamePrefix: platformKey.GetInstanceNamePrefix().String(),
		Platform:           platformKey.GetPlatformString(),
		SizeClass:          scq.sizeClass,
		TargetID:           truncateForTaskLog(t.targetID),
		Tokens:             t.getTaskLogTokens(),
		DoNotCache:         t.desiredState.Action.GetDoNotCache(),
	}
	for _, o := range t.operations {
		line.BackgroundLearning = line.BackgroundLearning || o.mayExistWithoutWaiters
	}
	bq.logTask(bq.now, line)
}

func (bq *InMemoryBuildQueue) logTaskAttached(o *operation, requestMetadata *remoteexecution.RequestMetadata, deduplicated bool) {
	if bq.configuration.TaskLog == nil {
		return
	}
	bq.logTask(bq.now, &TaskLogLine{
		Ev:                      "attached",
		Task:                    o.task.logID,
		Operation:               o.name,
		Priority:                o.priority,
		Deduplicated:            deduplicated,
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
		SizeClass:         t.getCurrentSizeClassQueue().sizeClass,
		CompletedByWorker: completedByWorker,
		Result:            result,
		GRPCCode:          grpcCode,
		StatusMessage:     truncateForTaskLog(executeResponse.GetStatus().GetMessage()),
		ExitCode:          executeResponse.GetResult().GetExitCode(),
		WorkerRetries:     t.retryCount,
	}
	if workerKey := t.currentWorker.workerKey; workerKey != "" {
		line.Worker = json.RawMessage(workerKey)
	}
	bq.logTask(bq.now, line)
}

// logTaskDetached must run before the operation is removed.
func (bq *InMemoryBuildQueue) logTaskDetached(o *operation) {
	if bq.configuration.TaskLog == nil {
		return
	}
	ev := "detached"
	if len(o.task.operations) == 1 {
		ev = "removed"
	}
	bq.logTask(bq.now, &TaskLogLine{
		Ev:        ev,
		Task:      o.task.logID,
		Operation: o.name,
	})
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
// are still buffered.
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
