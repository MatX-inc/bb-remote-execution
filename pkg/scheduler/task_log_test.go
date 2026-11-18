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
		if line.Ev != "start" {
			out = append(out, taskLogLineJSON(t, line))
		}
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

// Have every test in this package write the task log, so that
// task_log_check_test.go compares it against the build queue.
func init() {
	buildQueueConfigurationForTesting.TaskLog = func(*scheduler.TaskLogLine) {}
	tokenBuildQueueConfigurationForTesting.TaskLog = func(*scheduler.TaskLogLine) {}
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
	taskLogQueuedTask1 = `{"ev":"queued","task":1,"action_hash":"00000000000000000000000000000001","action_size_bytes":123,"instance_name":"main","instance_name_prefix":"main","target_id":"//pkg:target","tokens":[{"name":"oranges","amount":1}]}`
	taskLogQueuedTask2 = `{"ev":"queued","task":2,"action_hash":"00000000000000000000000000000002","action_size_bytes":123,"instance_name":"main","instance_name_prefix":"main","target_id":"//pkg:target","tokens":[{"name":"oranges","amount":1}]}`
)

// A task that waits for a token is logged as parked until the line
// that says it executes, which follows the line of the task whose token
// it takes. Every operation names its invocation.
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

	env.advance(time.Second)
	requireExecuting(t, env.synchronize("worker1", platformForTesting, 0, completedWorkerState(tokenActionHash(1), successfulExecuteResponse())), tokenActionHash(2))

	require.Equal(t, []string{
		taskLogQueuedTask1,
		`{"ev":"attached","task":1,"operation":"00000000-0000-0000-0000-000000000001","tool_invocation_id":"invocation1","correlated_invocations_id":"correlated-invocation1","action_mnemonic":"TestRunner"}`,
		`{"ev":"executing","task":1,"tokens":[{"name":"oranges","amount":1}],"worker":{"hostname":"worker1"}}`,
		taskLogQueuedTask2,
		`{"ev":"parked","task":2,"token":"oranges"}`,
		`{"ev":"attached","task":2,"operation":"00000000-0000-0000-0000-000000000002","tool_invocation_id":"invocation2","correlated_invocations_id":"correlated-invocation2","action_mnemonic":"TestRunner"}`,
		`{"ev":"attached","task":2,"operation":"00000000-0000-0000-0000-000000000003","tool_invocation_id":"invocation3","correlated_invocations_id":"correlated-invocation3","action_mnemonic":"TestRunner"}`,
		`{"ev":"finished","task":1,"worker":{"hostname":"worker1"},"completed_by_worker":true,"result":"Success"}`,
		`{"ev":"executing","task":2,"tokens":[{"name":"oranges","amount":1}],"worker":{"hostname":"worker1"}}`,
	}, recorder.taskLines(t))

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
	}, taskLines[len(taskLines)-3:])
}

// A queued task that no worker took is removed the same way.
func TestInMemoryBuildQueueTaskLogAbandonQueued(t *testing.T) {
	env, recorder := newTaskLogTestEnv(t)
	requireIdle(t, env.synchronize("worker1", platformForTesting, 0, idleWorkerState()))

	// Strings chosen by the client are cut to 1024 bytes.
	ctxWithCancel, cancel := context.WithCancel(env.ctx)
	stream := env.executeAs(ctxWithCancel, &executeOptions{
		hash:          tokenActionHash(1),
		operationName: tokenOperationName(1),
		instanceName:  "main/tenant",
		priority:      7,
	}, strings.Repeat("i", 1025))
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
		`{"ev":"queued","task":1,"action_hash":"00000000000000000000000000000001","action_size_bytes":123,"instance_name":"main/tenant","instance_name_prefix":"main","target_id":"//pkg:target"}`,
		`{"ev":"attached","task":1,"operation":"00000000-0000-0000-0000-000000000001","priority":7,"tool_invocation_id":"` + strings.Repeat("i", 1024) + `","correlated_invocations_id":"correlated-` + strings.Repeat("i", 1013) + `","action_mnemonic":"TestRunner"}`,
		`{"ev":"finished","task":1,"result":"Failure","grpc_code":"Canceled","status_message":"Task no longer has any waiting clients"}`,
	}, recorder.taskLines(t))
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
	requireExecuting(t, env.synchronize("large", platformForTesting, 8, idleWorkerState()), tokenActionHash(1))

	learner2.EXPECT().Succeeded(gomock.Any(), []uint32{3, 8}).Return(0, time.Duration(0), time.Duration(0), nil)
	env.advance(time.Second)
	requireIdle(t, env.synchronize("large", platformForTesting, 8, completedWorkerState(tokenActionHash(1), successfulExecuteResponse())))

	taskLines := recorder.taskLines(t)
	require.Equal(t, []string{
		`{"ev":"executing","task":1,"size_class":3,"tokens":[{"name":"oranges","amount":1}],"worker":{"hostname":"small"}}`,
		`{"ev":"finished","task":1,"worker":{"hostname":"small"},"completed_by_worker":true,"result":"NonZeroExitCode"}`,
		strings.Replace(taskLogQueuedTask1, `,"tokens"`, `,"size_class":8,"tokens"`, 1),
		`{"ev":"executing","task":1,"size_class":8,"tokens":[{"name":"oranges","amount":1}],"worker":{"hostname":"large"}}`,
		`{"ev":"finished","task":1,"worker":{"hostname":"large"},"completed_by_worker":true,"result":"Success"}`,
	}, taskLines[2:])
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
}

// A parked task that comes to wait for another pool is parked again.
func TestInMemoryBuildQueueTaskLogParkedOnOtherPool(t *testing.T) {
	env, recorder := newTaskLogTestEnv(t)
	env.registerPool("apples", 1)
	env.registerPool("oranges", 1)
	requireIdle(t, env.synchronize("worker1", platformForTesting, 0, idleWorkerState()))
	requireIdle(t, env.synchronize("worker2", platformForTesting, 0, idleWorkerState()))

	stream1 := env.executeAs(env.ctx, &executeOptions{
		hash:          tokenActionHash(1),
		operationName: tokenOperationName(1),
		tokens:        map[string]uint32{"apples": 1},
	}, "invocation1")
	requireOperationStage(t, stream1, tokenOperationName(1), tokenActionHash(1), remoteexecution.ExecutionStage_QUEUED)
	requireExecuting(t, env.synchronize("worker1", platformForTesting, 0, idleWorkerState()), tokenActionHash(1))

	stream2 := env.executeAs(env.ctx, &executeOptions{
		hash:          tokenActionHash(2),
		operationName: tokenOperationName(2),
		tokens:        map[string]uint32{"apples": 1, "oranges": 1},
	}, "invocation2")
	requireOperationStage(t, stream2, tokenOperationName(2), tokenActionHash(2), remoteexecution.ExecutionStage_QUEUED)

	stream3 := env.executeAs(env.ctx, &executeOptions{
		hash:          tokenActionHash(3),
		operationName: tokenOperationName(3),
		tokens:        map[string]uint32{"oranges": 1},
	}, "invocation3")
	requireOperationStage(t, stream3, tokenOperationName(3), tokenActionHash(3), remoteexecution.ExecutionStage_QUEUED)
	requireExecuting(t, env.synchronize("worker2", platformForTesting, 0, idleWorkerState()), tokenActionHash(3))

	// The apples come free, but the oranges are now taken.
	requireIdle(t, env.synchronize("worker1", platformForTesting, 0, completedWorkerState(tokenActionHash(1), successfulExecuteResponse())))

	taskLines := recorder.taskLines(t)
	require.Equal(t, []string{
		`{"ev":"parked","task":2,"token":"apples"}`,
		`{"ev":"finished","task":1,"worker":{"hostname":"worker1"},"completed_by_worker":true,"result":"Success"}`,
		`{"ev":"parked","task":2,"token":"oranges"}`,
	}, []string{taskLines[4], taskLines[len(taskLines)-2], taskLines[len(taskLines)-1]})
}

// During the startup grace period tasks are parked on the first pool
// they need, though it has tokens left.
func TestInMemoryBuildQueueTaskLogStartupGracePeriod(t *testing.T) {
	recorder := &taskLogRecorder{}
	configuration := tokenBuildQueueConfigurationForTesting
	configuration.TaskLog = recorder.record
	configuration.TokenPoolStartupGracePeriod = 20 * time.Second
	env := newTokenTestEnv(t, &configuration, time.Unix(1000, 0))
	env.registerPool("oranges", 1)
	requireIdle(t, env.synchronize("worker1", platformForTesting, 0, idleWorkerState()))

	stream := env.executeAs(env.ctx, &executeOptions{
		hash:          tokenActionHash(1),
		operationName: tokenOperationName(1),
		tokens:        map[string]uint32{"oranges": 1},
	}, "invocation1")
	requireOperationStage(t, stream, tokenOperationName(1), tokenActionHash(1), remoteexecution.ExecutionStage_QUEUED)

	lines := recorder.all()
	require.Equal(t, `{"ev":"start","token_pool_startup_grace_period_seconds":20}`, taskLogLineJSON(t, lines[0]))
	require.Equal(t, uint64(1), lines[0].Seq)
	require.Equal(t, int64(1000000), lines[0].Boot)
	require.Equal(t, `{"ev":"parked","task":1,"token":"oranges"}`, taskLogLineJSON(t, lines[2]))
}

// Without a task log, the hooks do nothing.
func TestInMemoryBuildQueueTaskLogOff(t *testing.T) {
	configuration := tokenBuildQueueConfigurationForTesting
	configuration.TaskLog = nil
	env := newTokenTestEnv(t, &configuration, time.Unix(1000, 0))
	env.registerPool("oranges", 1)
	requireIdle(t, env.synchronize("worker1", platformForTesting, 0, idleWorkerState()))

	env.execute(&executeOptions{
		hash:          tokenActionHash(1),
		operationName: tokenOperationName(1),
		tokens:        map[string]uint32{"oranges": 1},
	})
	requireExecuting(t, env.synchronize("worker1", platformForTesting, 0, idleWorkerState()), tokenActionHash(1))
	env.execute(&executeOptions{
		hash:          tokenActionHash(2),
		operationName: tokenOperationName(2),
		tokens:        map[string]uint32{"oranges": 1},
	})
	requireExecuting(t, env.synchronize("worker1", platformForTesting, 0, completedWorkerState(tokenActionHash(1), successfulExecuteResponse())), tokenActionHash(2))
	env.requirePool("oranges", 1, 0)
}

// The writer never blocks its caller: lines that do not fit in its
// buffer are dropped. The others are written as JSON lines, also when
// they are still buffered once the context is done.
func TestTaskLogWriter(t *testing.T) {
	var out bytes.Buffer
	writer := scheduler.NewTaskLogWriter(&out, 2)
	for seq := uint64(1); seq <= 3; seq++ {
		writer.Write(&scheduler.TaskLogLine{Seq: seq, Boot: 7, T: "t", Ev: "finished", Task: seq})
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	writer.Run(ctx)
	require.Equal(t, `{"seq":1,"boot":7,"t":"t","ev":"finished","task":1}`+"\n"+
		`{"seq":2,"boot":7,"t":"t","ev":"finished","task":2}`+"\n", out.String())
}

type blockingWriter struct{}

func (blockingWriter) Write(p []byte) (int, error) {
	select {}
}

func TestTaskLogWriterBlockedSink(t *testing.T) {
	writer := scheduler.NewTaskLogWriter(blockingWriter{}, 2)
	writer.Write(&scheduler.TaskLogLine{Seq: 1})

	// Shutdown must not wait for a sink that never takes the line.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	writer.Run(ctx)
}
