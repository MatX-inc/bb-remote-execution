package scheduler_test

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"sync"
	"testing"
	"time"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-remote-execution/internal/mock"
	"github.com/buildbarn/bb-remote-execution/pkg/scheduler"
	"github.com/stretchr/testify/require"

	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"

	"go.uber.org/mock/gomock"
)

// taskLogRecorder is the TaskLog of the build queue under test.
type taskLogRecorder struct {
	lock  sync.Mutex
	lines []*scheduler.TaskLogLine
}

func (r *taskLogRecorder) record(line *scheduler.TaskLogLine) {
	r.lock.Lock()
	defer r.lock.Unlock()
	r.lines = append(r.lines, line)
}

func (r *taskLogRecorder) all() []*scheduler.TaskLogLine {
	r.lock.Lock()
	defer r.lock.Unlock()
	return append([]*scheduler.TaskLogLine(nil), r.lines...)
}

// taskLines returns the lines about tasks as JSON, without the fields
// every line has.
func (r *taskLogRecorder) taskLines(t *testing.T) []string {
	var out []string
	for _, line := range r.all() {
		if line.Ev == "start" || line.Ev == "heartbeat" {
			continue
		}
		out = append(out, taskLogLineJSON(t, line))
	}
	return out
}

func taskLogLineJSON(t *testing.T, line *scheduler.TaskLogLine) string {
	stripped := *line
	stripped.Seq, stripped.Boot, stripped.T = 0, 0, ""
	data, err := json.Marshal(&stripped)
	require.NoError(t, err)
	s, ok := strings.CutPrefix(string(data), `{"seq":0,"boot":0,"t":"",`)
	require.True(t, ok)
	return "{" + s
}

// requireReplayMatchesPools replays the lines and checks what every
// consumer relies on: sequence numbers without gaps, and tokens held
// according to the lines equal to what the scheduler reports.
func (r *taskLogRecorder) requireReplayMatchesPools(env *tokenTestEnv, pools ...string) {
	t := env.t
	tokens := map[uint64][]scheduler.TaskLogTokens{}
	executing := map[uint64]bool{}
	inUse := map[string]uint32{}
	for i, line := range r.all() {
		require.Equal(t, uint64(i+1), line.Seq)
		require.Equal(t, int64(1000000), line.Boot)
		switch line.Ev {
		case "queued":
			tokens[line.Task] = line.Tokens
		case "executing":
			require.False(t, executing[line.Task])
			require.Equal(t, tokens[line.Task], line.Tokens)
			executing[line.Task] = true
			for _, r := range tokens[line.Task] {
				inUse[r.Name] += r.Amount
				require.LessOrEqual(t, inUse[r.Name], env.pool(r.Name).Capacity, "line %d holds more tokens of pool %s than exist", line.Seq, r.Name)
			}
		case "finished":
			if executing[line.Task] {
				delete(executing, line.Task)
				for _, r := range tokens[line.Task] {
					inUse[r.Name] -= r.Amount
				}
			}
		}
	}
	for _, name := range pools {
		require.Equal(t, env.pool(name).InUse, inUse[name], "tokens of pool %s held according to the task log", name)
	}
}

func newTaskLogTestEnv(t *testing.T) (*tokenTestEnv, *taskLogRecorder) {
	recorder := &taskLogRecorder{}
	configuration := tokenBuildQueueConfigurationForTesting
	configuration.TaskLog = recorder.record
	return newTokenTestEnv(t, &configuration, time.Unix(1000, 0)), recorder
}

// executeAs performs an Execute() call that carries the request
// metadata Bazel sends, without waiting for its first update.
func (env *tokenTestEnv) executeAs(ctx context.Context, o *executeOptions, toolInvocationID string) remoteexecution.Execution_ExecuteClient {
	requestMetadataBin, err := proto.Marshal(&remoteexecution.RequestMetadata{
		ToolInvocationId:        toolInvocationID,
		CorrelatedInvocationsId: "correlated-" + toolInvocationID,
		ActionMnemonic:          "TestRunner",
		TargetId:                "//pkg:target",
	})
	require.NoError(env.t, err)
	stream, err := env.executeStream(
		metadata.AppendToOutgoingContext(ctx, "build.bazel.remote.execution.v2.requestmetadata-bin", string(requestMetadataBin)),
		o,
	)
	require.NoError(env.t, err)
	return stream
}

const (
	taskLogQueuedTask1 = `{"ev":"queued","task":1,"action_hash":"00000000000000000000000000000001","action_size_bytes":123,"digest_function":"MD5","instance_name":"main","instance_name_prefix":"main","platform":"{\"properties\":[{\"name\":\"cpu\",\"value\":\"armv6\"},{\"name\":\"os\",\"value\":\"linux\"}]}","target_id":"//pkg:target","tokens":[{"name":"oranges","amount":1}]}`
	taskLogQueuedTask2 = `{"ev":"queued","task":2,"action_hash":"00000000000000000000000000000002","action_size_bytes":123,"digest_function":"MD5","instance_name":"main","instance_name_prefix":"main","platform":"{\"properties\":[{\"name\":\"cpu\",\"value\":\"armv6\"},{\"name\":\"os\",\"value\":\"linux\"}]}","target_id":"//pkg:target","tokens":[{"name":"oranges","amount":1}]}`
)

// A task that waits for a token is logged as parked until the line
// that says it executes, and every Execute() call names its invocation.
func TestInMemoryBuildQueueTaskLogTokenWait(t *testing.T) {
	env, recorder := newTaskLogTestEnv(t)
	env.registerPool("oranges", 1)
	requireIdle(t, env.synchronize("worker1", platformForTesting, 0, idleWorkerState()))

	stream1 := env.executeAs(env.ctx, &executeOptions{
		hash:          tokenActionHash(1),
		operationName: tokenOperationName(1),
		tokens:        map[string]uint32{"oranges": 1},
	}, "invocation1")
	requireOperationStage(t, stream1, tokenOperationName(1), tokenActionHash(1), remoteexecution.ExecutionStage_QUEUED)
	requireExecuting(t, env.synchronize("worker1", platformForTesting, 0, idleWorkerState()), tokenActionHash(1))

	env.advance(time.Second)
	stream2 := env.executeAs(env.ctx, &executeOptions{
		hash:          tokenActionHash(2),
		operationName: tokenOperationName(2),
		tokens:        map[string]uint32{"oranges": 1},
	}, "invocation2")
	requireOperationStage(t, stream2, tokenOperationName(2), tokenActionHash(2), remoteexecution.ExecutionStage_QUEUED)

	// A second invocation asks for the action the parked task runs.
	stream3 := env.executeAs(env.ctx, &executeOptions{
		hash:              tokenActionHash(2),
		operationName:     tokenOperationName(3),
		tokens:            map[string]uint32{"oranges": 1},
		invocationID:      "e4896008-d596-44c7-8df6-6ced53dff6b0",
		selectorAbandoned: true,
	}, "invocation3")
	requireOperationStage(t, stream3, tokenOperationName(3), tokenActionHash(2), remoteexecution.ExecutionStage_QUEUED)
	recorder.requireReplayMatchesPools(env, "oranges")

	env.advance(time.Second)
	requireExecuting(t, env.synchronize("worker1", platformForTesting, 0, completedWorkerState(tokenActionHash(1), successfulExecuteResponse())), tokenActionHash(2))

	require.Equal(t, []string{
		taskLogQueuedTask1,
		`{"ev":"attached","task":1,"operation":"00000000-0000-0000-0000-000000000001","tool_invocation_id":"invocation1","correlated_invocations_id":"correlated-invocation1","action_mnemonic":"TestRunner"}`,
		`{"ev":"executing","task":1,"tokens":[{"name":"oranges","amount":1}],"worker":{"hostname":"worker1"}}`,
		taskLogQueuedTask2,
		`{"ev":"parked","task":2,"token":"oranges"}`,
		`{"ev":"attached","task":2,"operation":"00000000-0000-0000-0000-000000000002","tool_invocation_id":"invocation2","correlated_invocations_id":"correlated-invocation2","action_mnemonic":"TestRunner"}`,
		`{"ev":"attached","task":2,"operation":"00000000-0000-0000-0000-000000000003","deduplicated":true,"tool_invocation_id":"invocation3","correlated_invocations_id":"correlated-invocation3","action_mnemonic":"TestRunner"}`,
		`{"ev":"finished","task":1,"worker":{"hostname":"worker1"},"completed_by_worker":true,"result":"Success"}`,
		`{"ev":"executing","task":2,"tokens":[{"name":"oranges","amount":1}],"worker":{"hostname":"worker1"}}`,
	}, recorder.taskLines(t))
	recorder.requireReplayMatchesPools(env, "oranges")

	lines := recorder.all()
	require.Equal(t, "start", lines[0].Ev)
	require.Equal(t, "1970-01-01T00:16:40.000000Z", lines[0].T)
	require.Equal(t, "1970-01-01T00:16:42.000000Z", lines[len(lines)-1].T)
}

// A task that is removed without having executed finishes without a
// worker. The temporary worker task.complete() uses is not logged.
func TestInMemoryBuildQueueTaskLogAbandonParked(t *testing.T) {
	env, recorder := newTaskLogTestEnv(t)
	env.registerPool("oranges", 1)
	requireIdle(t, env.synchronize("worker1", platformForTesting, 0, idleWorkerState()))

	stream1 := env.executeAs(env.ctx, &executeOptions{
		hash:          tokenActionHash(1),
		operationName: tokenOperationName(1),
		tokens:        map[string]uint32{"oranges": 1},
	}, "invocation1")
	requireOperationStage(t, stream1, tokenOperationName(1), tokenActionHash(1), remoteexecution.ExecutionStage_QUEUED)
	requireExecuting(t, env.synchronize("worker1", platformForTesting, 0, idleWorkerState()), tokenActionHash(1))

	env.advance(time.Second)
	ctxWithCancel, cancel := context.WithCancel(env.ctx)
	stream2 := env.executeAs(ctxWithCancel, &executeOptions{
		hash:          tokenActionHash(2),
		operationName: tokenOperationName(2),
		tokens:        map[string]uint32{"oranges": 1},
	}, "invocation2")
	requireOperationStage(t, stream2, tokenOperationName(2), tokenActionHash(2), remoteexecution.ExecutionStage_QUEUED)

	cancel()
	require.Eventually(t, func() bool {
		return env.operation(tokenOperationName(2)).Timeout != nil
	}, 10*time.Second, time.Millisecond)
	env.advance(30 * time.Second)
	env.synchronize("worker1", platformForTesting, 0, executingWorkerState(tokenActionHash(1)))
	env.advance(35 * time.Second)
	require.Equal(t, []string{tokenOperationName(1)}, env.listOperations(remoteexecution.ExecutionStage_UNKNOWN, ""))

	taskLines := recorder.taskLines(t)
	require.Equal(t, []string{
		`{"ev":"parked","task":2,"token":"oranges"}`,
		`{"ev":"attached","task":2,"operation":"00000000-0000-0000-0000-000000000002","tool_invocation_id":"invocation2","correlated_invocations_id":"correlated-invocation2","action_mnemonic":"TestRunner"}`,
		`{"ev":"finished","task":2,"result":"Failure","grpc_code":"Canceled","status_message":"Task no longer has any waiting clients"}`,
		`{"ev":"removed","task":2,"operation":"00000000-0000-0000-0000-000000000002"}`,
	}, taskLines[len(taskLines)-4:])
	recorder.requireReplayMatchesPools(env, "oranges")
}

// A queued task that no worker took is removed the same way.
func TestInMemoryBuildQueueTaskLogAbandonQueued(t *testing.T) {
	env, recorder := newTaskLogTestEnv(t)
	requireIdle(t, env.synchronize("worker1", platformForTesting, 0, idleWorkerState()))

	ctxWithCancel, cancel := context.WithCancel(env.ctx)
	stream := env.executeAs(ctxWithCancel, &executeOptions{
		hash:          tokenActionHash(1),
		operationName: tokenOperationName(1),
	}, "invocation1")
	requireOperationStage(t, stream, tokenOperationName(1), tokenActionHash(1), remoteexecution.ExecutionStage_QUEUED)

	cancel()
	require.Eventually(t, func() bool {
		return env.operation(tokenOperationName(1)).Timeout != nil
	}, 10*time.Second, time.Millisecond)
	env.advance(30 * time.Second)
	requireIdle(t, env.synchronizeWithPreference("worker1", platformForTesting, 0, idleWorkerState(), true))
	env.advance(35 * time.Second)
	require.Empty(t, env.listOperations(remoteexecution.ExecutionStage_UNKNOWN, ""))

	require.Equal(t, []string{
		`{"ev":"queued","task":1,"action_hash":"00000000000000000000000000000001","action_size_bytes":123,"digest_function":"MD5","instance_name":"main","instance_name_prefix":"main","platform":"{\"properties\":[{\"name\":\"cpu\",\"value\":\"armv6\"},{\"name\":\"os\",\"value\":\"linux\"}]}","target_id":"//pkg:target"}`,
		`{"ev":"attached","task":1,"operation":"00000000-0000-0000-0000-000000000001","tool_invocation_id":"invocation1","correlated_invocations_id":"correlated-invocation1","action_mnemonic":"TestRunner"}`,
		`{"ev":"finished","task":1,"result":"Failure","grpc_code":"Canceled","status_message":"Task no longer has any waiting clients"}`,
		`{"ev":"removed","task":1,"operation":"00000000-0000-0000-0000-000000000001"}`,
	}, recorder.taskLines(t))
	recorder.requireReplayMatchesPools(env)
}

// A task that fails on a small size class is queued again under the
// same ID on the largest one.
func TestInMemoryBuildQueueTaskLogSizeClassRetry(t *testing.T) {
	env, recorder := newTaskLogTestEnv(t)
	env.registerPool("oranges", 1)
	env.registerPlatformQueue(platformForTesting, 0, []uint32{8})
	requireIdle(t, env.synchronize("small", platformForTesting, 3, idleWorkerState()))
	requireIdle(t, env.synchronize("large", platformForTesting, 8, idleWorkerState()))

	learner1 := mock.NewMockLearner(env.ctrl)
	stream := env.executeAs(env.ctx, &executeOptions{
		hash:          tokenActionHash(1),
		operationName: tokenOperationName(1),
		tokens:        map[string]uint32{"oranges": 1},
		sizeClasses:   []uint32{3, 8},
		learner:       learner1,
	}, "invocation1")
	requireOperationStage(t, stream, tokenOperationName(1), tokenActionHash(1), remoteexecution.ExecutionStage_QUEUED)
	requireExecuting(t, env.synchronize("small", platformForTesting, 3, idleWorkerState()), tokenActionHash(1))

	learner2 := mock.NewMockLearner(env.ctrl)
	learner1.EXPECT().Failed(false).Return(2*time.Minute, 5*time.Minute, learner2)
	env.advance(time.Second)
	requireIdle(t, env.synchronize("small", platformForTesting, 3, completedWorkerState(tokenActionHash(1), &remoteexecution.ExecuteResponse{
		Result: &remoteexecution.ActionResult{ExitCode: 1},
	})))
	recorder.requireReplayMatchesPools(env, "oranges")
	requireExecuting(t, env.synchronize("large", platformForTesting, 8, idleWorkerState()), tokenActionHash(1))

	learner2.EXPECT().Succeeded(gomock.Any(), []uint32{3, 8}).Return(0, time.Duration(0), time.Duration(0), nil)
	env.advance(time.Second)
	requireIdle(t, env.synchronize("large", platformForTesting, 8, completedWorkerState(tokenActionHash(1), successfulExecuteResponse())))

	taskLines := recorder.taskLines(t)
	require.Equal(t, []string{
		`{"ev":"executing","task":1,"size_class":3,"tokens":[{"name":"oranges","amount":1}],"worker":{"hostname":"small"}}`,
		`{"ev":"finished","task":1,"size_class":3,"worker":{"hostname":"small"},"completed_by_worker":true,"result":"NonZeroExitCode","exit_code":1}`,
		strings.Replace(taskLogQueuedTask1, `,"tokens"`, `,"size_class":8,"tokens"`, 1),
		`{"ev":"executing","task":1,"size_class":8,"tokens":[{"name":"oranges","amount":1}],"worker":{"hostname":"large"}}`,
		`{"ev":"finished","task":1,"size_class":8,"worker":{"hostname":"large"},"completed_by_worker":true,"result":"Success"}`,
	}, taskLines[2:])
	recorder.requireReplayMatchesPools(env, "oranges")
}

// A worker that disappears ends the attempt without completing it.
func TestInMemoryBuildQueueTaskLogStaleWorker(t *testing.T) {
	env, recorder := newTaskLogTestEnv(t)
	env.registerPool("oranges", 1)
	requireIdle(t, env.synchronize("worker1", platformForTesting, 0, idleWorkerState()))

	stream := env.executeAs(env.ctx, &executeOptions{
		hash:          tokenActionHash(1),
		operationName: tokenOperationName(1),
		tokens:        map[string]uint32{"oranges": 1},
	}, "invocation1")
	requireOperationStage(t, stream, tokenOperationName(1), tokenActionHash(1), remoteexecution.ExecutionStage_QUEUED)
	requireExecuting(t, env.synchronize("worker1", platformForTesting, 0, idleWorkerState()), tokenActionHash(1))

	env.advance(2 * time.Minute)
	env.requirePool("oranges", 0, 0)

	taskLines := recorder.taskLines(t)
	require.Equal(t,
		`{"ev":"finished","task":1,"worker":{"hostname":"worker1"},"result":"Failure","grpc_code":"Unavailable","status_message":"Worker {\"hostname\":\"worker1\"} disappeared while task was executing"}`,
		taskLines[len(taskLines)-1])
	recorder.requireReplayMatchesPools(env, "oranges")
}

// Heartbeats are written at most once per call into the scheduler, and
// carry the token pools.
func TestInMemoryBuildQueueTaskLogHeartbeat(t *testing.T) {
	env, recorder := newTaskLogTestEnv(t)
	env.registerPool("oranges", 2)
	require.Equal(t, []string{
		`{"ev":"start","heartbeat_seconds":10}`,
	}, taskLogLinesJSON(t, recorder.all()))

	env.advance(25 * time.Second)
	env.requirePool("oranges", 0, 0)
	env.requirePool("oranges", 0, 0)
	env.advance(10 * time.Second)
	env.requirePool("oranges", 0, 0)
	require.Equal(t, []string{
		`{"ev":"start","heartbeat_seconds":10}`,
		`{"ev":"heartbeat","token_pools":[{"instance_name_prefix":"main","name":"oranges","capacity":2}]}`,
		`{"ev":"heartbeat","token_pools":[{"instance_name_prefix":"main","name":"oranges","capacity":2}]}`,
	}, taskLogLinesJSON(t, recorder.all()))
	recorder.requireReplayMatchesPools(env, "oranges")
}

func taskLogLinesJSON(t *testing.T, lines []*scheduler.TaskLogLine) []string {
	out := make([]string, 0, len(lines))
	for _, line := range lines {
		out = append(out, taskLogLineJSON(t, line))
	}
	return out
}

type lockedBuffer struct {
	lock sync.Mutex
	b    bytes.Buffer
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.lock.Lock()
	defer b.lock.Unlock()
	return b.b.Write(p)
}

func (b *lockedBuffer) String() string {
	b.lock.Lock()
	defer b.lock.Unlock()
	return b.b.String()
}

// The writer never blocks its caller: lines that do not fit in its
// buffer are dropped, and the others are written as JSON lines.
func TestTaskLogWriter(t *testing.T) {
	var out lockedBuffer
	writer := scheduler.NewTaskLogWriter(&out, 2)
	for seq := uint64(1); seq <= 3; seq++ {
		writer.Write(&scheduler.TaskLogLine{Seq: seq, Boot: 7, T: "t", Ev: "removed", Task: seq})
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		writer.Run(ctx)
		close(done)
	}()
	const expected = `{"seq":1,"boot":7,"t":"t","ev":"removed","task":1}` + "\n" +
		`{"seq":2,"boot":7,"t":"t","ev":"removed","task":2}` + "\n"
	require.Eventually(t, func() bool {
		return out.String() == expected
	}, 10*time.Second, time.Millisecond)
	cancel()
	<-done
	require.Equal(t, expected, out.String())
}
