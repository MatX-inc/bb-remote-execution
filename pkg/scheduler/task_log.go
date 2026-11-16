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

// The task log is one line of JSON for every change in the state of a
// task: enough to reconstruct, at any past moment, which tasks were
// queued, parked on a token pool or executing, on which worker, and on
// behalf of which invocations. BuildQueueState only shows the present.
//
// Lines are numbered per scheduler process, in the order in which the
// changes happened. A consumer orders them by (Boot, Seq) and drops
// duplicates; a gap in Seq is a lost line. T is the time of the call
// into the scheduler that caused the change, so lines of one call share
// it.
//
// A scheduler restart loses every task. The next process logs "start"
// under a new Boot, at which point everything left open by the previous
// one has ended.
//
// Nothing repeats a lost line. A task whose last lines were lost stays
// open in a replay until the next "start"; the token pools in the
// heartbeats are what the scheduler itself counted.

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

// TaskLogLine is a single line of the task log. Seq, Boot, T and Ev
// are set on every line; which other fields are set depends on Ev. A
// number or boolean that is absent from the JSON is zero or false.
//
//   - "start": the scheduler process started. While the token pool
//     startup grace period lasts, tasks that require tokens are parked
//     regardless of the state of their pools.
//   - "heartbeat": written every ten seconds while the scheduler
//     receives calls. Carries the token pools, against which a consumer
//     can check the tokens it believes to be held, and the number of
//     tasks logged so far.
//   - "queued": a task entered the QUEUED stage, either for the first
//     time or to be retried on the largest size class. Describes the
//     task.
//   - "attached": an Execute() call started waiting for a task. Names
//     the invocation, which the other lines don't.
//   - "parked": a queued task started waiting for a token pool. It
//     stops waiting at its next "executing" or "finished" line. Token
//     is the pool that was short at that moment; the scheduler may
//     report the task on another of its pools later, so the blocked
//     counts of a heartbeat match a replay only in total.
//   - "executing": a worker was assigned the task, and the task took
//     its tokens.
//   - "finished": the attempt ended, and its tokens were returned.
//     Worker is absent if the task never executed. Result and GRPCCode
//     are those of the tasks_executing_duration_seconds metric. If a
//     "queued" line for the same task follows, it is being retried.
//   - "detached": an operation was removed, while others keep the task.
//     This happens a while after its last client stopped waiting.
//   - "removed": the last operation of a task was removed, and with it
//     the task.
type TaskLogLine struct {
	Seq  uint64 `json:"seq"`
	Boot int64  `json:"boot"`
	T    string `json:"t"`
	Ev   string `json:"ev"`

	// Identifies a task within a Boot. Action digests don't: tasks
	// that must not be cached are not deduplicated.
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

	// "queued" and "executing": the tokens the task holds while
	// executing.
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

// taskLogState is the part of InMemoryBuildQueue that only the task
// log uses.
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

// logTask numbers a line and hands it to the configured sink. It is
// called with the lock held, so the sink must not block.
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

// logTaskAttached is called for every Execute() call that ends up
// waiting for a task. Deduplicated is set if the task already existed.
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

// logTaskFinished is called by task.complete() while the task still
// has its worker, before its tokens go to other tasks.
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

// logTaskDetached is called right before an operation is removed from
// its task.
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

// TaskLogWriter writes task log lines to an io.Writer from a goroutine
// of its own, so that InMemoryBuildQueue never waits for the sink. When
// the sink falls behind by more than the buffer, lines are dropped and
// counted; their sequence numbers go missing.
type TaskLogWriter struct {
	w     io.Writer
	lines chan *TaskLogLine
}

// NewTaskLogWriter creates a TaskLogWriter that buffers at most
// bufferedLines lines. Run() must be called for lines to be written.
func NewTaskLogWriter(w io.Writer, bufferedLines int) *TaskLogWriter {
	return &TaskLogWriter{
		w:     w,
		lines: make(chan *TaskLogLine, bufferedLines),
	}
}

// Write a line without blocking. This method can be used as
// InMemoryBuildQueueConfiguration.TaskLog.
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
