package scheduler

import (
	"context"
	"flag"
	"fmt"
	"io"
	"math/rand"
	"os"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-remote-execution/pkg/proto/buildqueuestate"
	"github.com/buildbarn/bb-remote-execution/pkg/proto/remoteworker"
	"github.com/buildbarn/bb-remote-execution/pkg/scheduler/initialsizeclass"
	scheduler_invocation "github.com/buildbarn/bb-remote-execution/pkg/scheduler/invocation"
	"github.com/buildbarn/bb-remote-execution/pkg/scheduler/platform"
	"github.com/buildbarn/bb-storage/pkg/auth"
	"github.com/buildbarn/bb-storage/pkg/blobstore"
	"github.com/buildbarn/bb-storage/pkg/blobstore/buffer"
	"github.com/buildbarn/bb-storage/pkg/clock"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/util"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/emptypb"

	"cloud.google.com/go/longrunning/autogen/longrunningpb"
)

// The tests in this file make random calls to a build queue. They rely
// on task_log_check_test.go and token_invariants_test.go, which panic
// when the task log or the token pools disagree with the build queue
// as its lock is released.

var (
	taskLogFuzzFirstSeed  = flag.Int64("task_log_fuzz_first_seed", 1, "First seed of TestInMemoryBuildQueueTaskLogRandomized")
	taskLogFuzzSeeds      = flag.Int64("task_log_fuzz_seeds", 8, "Number of seeds of TestInMemoryBuildQueueTaskLogRandomized")
	taskLogFuzzSteps      = flag.Int("task_log_fuzz_steps", 1000, "Number of calls TestInMemoryBuildQueueTaskLogRandomized makes per seed")
	taskLogFuzzConcurrent = flag.Duration("task_log_fuzz_concurrent", time.Second, "Duration of TestInMemoryBuildQueueTaskLogConcurrent")
)

const taskLogFuzzIdleWorkerSynchronizationInterval = 31 * time.Second

// taskLogFuzzClock is a clock the test advances. Every reading is a
// nanosecond later than the last, so that the order in which the build
// queue cleans up does not depend on how it breaks ties.
type taskLogFuzzClock struct {
	clock.Clock
	lock sync.Mutex
	now  time.Time
	// The timer of the last Synchronize() call that waited for work.
	lastIdleTimer chan time.Time
}

func (c *taskLogFuzzClock) Now() time.Time {
	return c.advance(time.Nanosecond)
}

func (c *taskLogFuzzClock) advance(d time.Duration) time.Time {
	c.lock.Lock()
	defer c.lock.Unlock()
	c.now = c.now.Add(d)
	return c.now
}

type taskLogFuzzTimer struct{}

func (taskLogFuzzTimer) Stop() bool { return true }

// NewTimer returns a timer the test fires for workers that wait for
// work, and one that never fires for clients that wait for updates.
func (c *taskLogFuzzClock) NewTimer(d time.Duration) (clock.Timer, <-chan time.Time) {
	if d != taskLogFuzzIdleWorkerSynchronizationInterval {
		return taskLogFuzzTimer{}, nil
	}
	c.lock.Lock()
	defer c.lock.Unlock()
	c.lastIdleTimer = make(chan time.Time, 1)
	return taskLogFuzzTimer{}, c.lastIdleTimer
}

// taskLogFuzzSelector is what the initial size class analyzer decides
// for one Execute() call.
type taskLogFuzzSelector struct {
	smallest, retry, background bool
}

type taskLogFuzzSelectorKey struct{}

func (s *taskLogFuzzSelector) Select(sizeClasses []uint32) (int, time.Duration, time.Duration, initialsizeclass.Learner) {
	if s.smallest {
		return 0, time.Minute, time.Minute, s
	}
	return len(sizeClasses) - 1, time.Minute, time.Minute, s
}

func (taskLogFuzzSelector) Abandoned() {}

func (s *taskLogFuzzSelector) Succeeded(time.Duration, []uint32) (int, time.Duration, time.Duration, initialsizeclass.Learner) {
	if s.background {
		return 0, time.Minute, time.Minute, &taskLogFuzzSelector{retry: s.retry}
	}
	return 0, 0, 0, nil
}

func (s *taskLogFuzzSelector) Failed(bool) (time.Duration, time.Duration, initialsizeclass.Learner) {
	if s.retry {
		return time.Minute, time.Minute, &taskLogFuzzSelector{}
	}
	return 0, 0, nil
}

type taskLogFuzzCAS struct {
	blobstore.BlobAccess
	actions map[string]*remoteexecution.Action
}

func (c taskLogFuzzCAS) Get(ctx context.Context, d digest.Digest) buffer.Buffer {
	// A message of its own for every call, as from a real CAS.
	return buffer.NewProtoBufferFromProto(proto.Clone(c.actions[d.GetHashString()]), buffer.UserProvided)
}

type taskLogFuzzRouter struct{}

func (taskLogFuzzRouter) RouteAction(ctx context.Context, digestFunction digest.Function, action *remoteexecution.Action, requestMetadata *remoteexecution.RequestMetadata) (*remoteexecution.Action, platform.Key, []scheduler_invocation.Key, initialsizeclass.Selector, error) {
	var invocationKeys []scheduler_invocation.Key
	if id := requestMetadata.GetToolInvocationId(); id != "" {
		invocationKeys = []scheduler_invocation.Key{util.Must(scheduler_invocation.NewKey(util.Must(anypb.New(&remoteexecution.RequestMetadata{ToolInvocationId: id}))))}
	}
	return action, platform.MustNewKey("main", action.Platform), invocationKeys, ctx.Value(taskLogFuzzSelectorKey{}).(*taskLogFuzzSelector), nil
}

type taskLogFuzzStream struct {
	grpc.ServerStream
	ctx     context.Context
	updates chan *longrunningpb.Operation
}

func (s *taskLogFuzzStream) Context() context.Context { return s.ctx }

func (s *taskLogFuzzStream) Send(o *longrunningpb.Operation) error {
	s.updates <- o
	return nil
}

const taskLogFuzzActions = 12

// Longer than the task log lets strings be.
var taskLogFuzzLongString = strings.Repeat("x", taskLogMaximumStringSizeBytes+1)

func taskLogFuzzHash(n int) string { return fmt.Sprintf("%032x", n) }

// newTaskLogFuzzExecute returns the arguments of a random Execute()
// call, and what the call is.
func newTaskLogFuzzExecute(ctx context.Context, random *rand.Rand) (*remoteexecution.ExecuteRequest, *taskLogFuzzStream, string) {
	hash := taskLogFuzzHash(random.Intn(taskLogFuzzActions))
	invocation := random.Intn(4)
	requestMetadata := &remoteexecution.RequestMetadata{
		CorrelatedInvocationsId: taskLogFuzzLongString,
		ActionMnemonic:          taskLogFuzzLongString,
		TargetId:                taskLogFuzzLongString,
	}
	if invocation > 0 {
		requestMetadata.ToolInvocationId = strconv.Itoa(invocation) + taskLogFuzzLongString
	}
	selector := &taskLogFuzzSelector{smallest: random.Intn(3) != 0, retry: random.Intn(2) == 0, background: random.Intn(3) == 0}
	ctx = metadata.NewIncomingContext(ctx, metadata.Pairs(
		"build.bazel.remote.execution.v2.requestmetadata-bin", string(util.Must(proto.Marshal(requestMetadata))),
	))
	return &remoteexecution.ExecuteRequest{
			InstanceName:    "main/" + taskLogFuzzLongString,
			ActionDigest:    &remoteexecution.Digest{Hash: hash, SizeBytes: 123},
			ExecutionPolicy: &remoteexecution.ExecutionPolicy{Priority: int32(random.Intn(3))},
		}, &taskLogFuzzStream{
			ctx:     context.WithValue(ctx, taskLogFuzzSelectorKey{}, selector),
			updates: make(chan *longrunningpb.Operation, 1024),
		}, fmt.Sprintf("execute %s as invocation %d %+v", hash[28:], invocation, *selector)
}

type taskLogFuzzClient struct {
	cancel    context.CancelFunc
	stream    *taskLogFuzzStream
	done      chan error
	operation string
}

type taskLogFuzzSynchronizeResult struct {
	response *remoteworker.SynchronizeResponse
	err      error
}

type taskLogFuzzWorker struct {
	name      string
	platform  *remoteexecution.Platform
	sizeClass uint32
	// The action the worker thinks it executes, if any.
	executing string
	// Set while a Synchronize() call waits for work.
	waiting chan taskLogFuzzSynchronizeResult
	timer   chan time.Time
}

// newRequest returns a random Synchronize() request, and what the
// worker reports through it.
func (w *taskLogFuzzWorker) newRequest(random *rand.Rand) (*remoteworker.SynchronizeRequest, string) {
	// The state of a worker that executes an action, or that has
	// completed it if a response is given.
	executing := func(hash string, response *remoteexecution.ExecuteResponse) *remoteworker.CurrentState {
		executing := &remoteworker.CurrentState_Executing{
			ActionDigest:   &remoteexecution.Digest{Hash: hash, SizeBytes: 123},
			ExecutionState: &remoteworker.CurrentState_Executing_FetchingInputs{FetchingInputs: &emptypb.Empty{}},
		}
		if response != nil {
			executing.ExecutionState = &remoteworker.CurrentState_Executing_Completed{Completed: response}
		}
		return &remoteworker.CurrentState{WorkerState: &remoteworker.CurrentState_Executing_{Executing: executing}}
	}
	exited := func(exitCode int32) *remoteexecution.ExecuteResponse {
		return &remoteexecution.ExecuteResponse{Result: &remoteexecution.ActionResult{ExitCode: exitCode}}
	}
	failed := func(code codes.Code) *remoteexecution.ExecuteResponse {
		return &remoteexecution.ExecuteResponse{Status: status.New(code, taskLogFuzzLongString).Proto()}
	}
	// The weights add up to 20.
	type report struct {
		weight int
		what   string
		state  *remoteworker.CurrentState
	}
	idle := &remoteworker.CurrentState{WorkerState: &remoteworker.CurrentState_Idle{Idle: &emptypb.Empty{}}}
	unknown := taskLogFuzzHash(taskLogFuzzActions)
	reports := []report{
		{18, "idle", idle},
		{2, "executing an action it was not given", executing(unknown, nil)},
	}
	if w.executing != "" {
		reports = []report{
			{8, "completed", executing(w.executing, exited(0))},
			{3, "completed with exit code 1", executing(w.executing, exited(1))},
			{1, "completed with an error", executing(w.executing, failed(codes.Internal))},
			{1, "timed out", executing(w.executing, failed(codes.DeadlineExceeded))},
			{1, "completed without a result", executing(w.executing, &remoteexecution.ExecuteResponse{})},
			{3, "executing", executing(w.executing, nil)},
			{2, "idle, having lost its action", idle},
			{1, "completed an action it was not given", executing(unknown, exited(0))},
		}
	}
	r := 0
	for n := random.Intn(20); n >= reports[r].weight; r++ {
		n -= reports[r].weight
	}
	return &remoteworker.SynchronizeRequest{
		WorkerId:           map[string]string{"hostname": w.name},
		InstanceNamePrefix: "main",
		Platform:           w.platform,
		SizeClass:          w.sizeClass,
		CurrentState:       reports[r].state,
		PreferBeingIdle:    w.executing == "" && random.Intn(8) == 0,
	}, reports[r].what
}

// handleResult makes the worker do what the scheduler told it, and
// returns what that is.
func (w *taskLogFuzzWorker) handleResult(result taskLogFuzzSynchronizeResult) string {
	if result.err != nil {
		return result.err.Error()
	}
	switch desired := result.response.DesiredState.GetWorkerState().(type) {
	case *remoteworker.DesiredState_Idle:
		w.executing = ""
		return "idle"
	case *remoteworker.DesiredState_Executing_:
		w.executing = desired.Executing.ActionDigest.Hash
		return "execute " + w.executing[28:]
	}
	return "continue"
}

func newTaskLogFuzzKillRequest(operation string) *buildqueuestate.KillOperationsRequest {
	return &buildqueuestate.KillOperationsRequest{
		Filter: &buildqueuestate.KillOperationsRequest_Filter{
			Type: &buildqueuestate.KillOperationsRequest_Filter_OperationName{OperationName: operation},
		},
		Status: status.New(codes.Aborted, "Fuzz").Proto(),
	}
}

type taskLogFuzzer struct {
	t       *testing.T
	seed    int64
	taskLog bool
	// Make the build queue answer the same calls the same way in
	// every run: let no Execute() call be deduplicated, as the build
	// queue walks the operations of a task in the order of a Go map,
	// and let at most one worker wait for work, as workers that are
	// woken up by the same call return in any order.
	repeatable bool

	random *rand.Rand
	ctx    context.Context
	cancel context.CancelFunc
	clock  *taskLogFuzzClock
	bq     *InMemoryBuildQueue

	workers []*taskLogFuzzWorker
	clients []*taskLogFuzzClient

	lastUUID uint64
	step     int
	// The calls made, their answers, and the state of the build
	// queue after every step.
	trace []string
	// The number of lines by event. Written with the build queue's
	// lock held.
	events map[string]int
}

func newTaskLogFuzzer(t *testing.T, seed int64, taskLog, repeatable bool) *taskLogFuzzer {
	f := &taskLogFuzzer{
		t:          t,
		seed:       seed,
		taskLog:    taskLog,
		repeatable: repeatable,
		random:     rand.New(rand.NewSource(seed)),
		clock:      &taskLogFuzzClock{now: time.Unix(1000, 0)},
		events:     map[string]int{},
	}
	f.ctx, f.cancel = context.WithCancel(context.Background())
	var platforms [2]*remoteexecution.Platform
	for i, cpu := range []string{"armv6", "x86_64"} {
		platforms[i] = &remoteexecution.Platform{Properties: []*remoteexecution.Platform_Property{
			{Name: "cpu", Value: cpu},
			{Name: "os", Value: "linux"},
		}}
	}
	// Actions with every kind of demand on two token pools. Some
	// run on the other platform, and some may not be deduplicated.
	cas := taskLogFuzzCAS{actions: map[string]*remoteexecution.Action{}}
	for i := range taskLogFuzzActions {
		properties := slices.Clone(platforms[i%4/3].Properties)
		if a := []int{0, 1, 2, 0, 0, 1, 0, 1, 0, 2, 0, 0}[i]; a > 0 {
			properties = append(properties, &remoteexecution.Platform_Property{Name: "token:a", Value: strconv.Itoa(a)})
		}
		if b := []int{0, 0, 0, 0, 1, 2, 3, 0, 0, 1, 0, 1}[i]; b > 0 {
			properties = append(properties, &remoteexecution.Platform_Property{Name: "token:b", Value: strconv.Itoa(b)})
		}
		cas.actions[taskLogFuzzHash(i)] = &remoteexecution.Action{
			CommandDigest: &remoteexecution.Digest{Hash: "61c585c297d00409bd477b6b80759c94ec545ab4", SizeBytes: 456},
			Platform:      &remoteexecution.Platform{Properties: properties},
			DoNotCache:    i%5 == 4 || repeatable,
		}
	}
	for _, w := range []taskLogFuzzWorker{
		{name: "small1", sizeClass: 1},
		{name: "small2", sizeClass: 1},
		{name: "small3", sizeClass: 1},
		{name: "large1", sizeClass: 4},
		{name: "large2", sizeClass: 4},
		{name: "other1", platform: platforms[1]},
		{name: "other2", platform: platforms[1]},
	} {
		if w.platform == nil {
			w.platform = platforms[0]
		}
		f.workers = append(f.workers, &w)
	}

	configuration := InMemoryBuildQueueConfiguration{
		ExecutionUpdateInterval:              time.Minute,
		OperationWithNoWaitersTimeout:        time.Minute,
		PlatformQueueWithNoWorkersTimeout:    3 * time.Minute,
		BusyWorkerSynchronizationInterval:    10 * time.Second,
		GetIdleWorkerSynchronizationInterval: func() time.Duration { return taskLogFuzzIdleWorkerSynchronizationInterval },
		WorkerTaskRetryCount:                 2,
		WorkerWithNoSynchronizationsTimeout:  time.Minute,
	}
	if f.random.Intn(2) == 0 {
		configuration.TokenPoolStartupGracePeriod = 45 * time.Second
	}
	if taskLog {
		// Write the lines the way bb_scheduler does, so that the
		// race detector sees them cross to another goroutine.
		writer := NewTaskLogWriter(io.Discard, 16)
		go writer.Run(f.ctx)
		configuration.TaskLog = func(line *TaskLogLine) {
			writer.Write(line)
			f.events[line.Ev]++
		}
	}
	allowAll := auth.NewStaticAuthorizer(func(digest.InstanceName) bool { return true })
	f.bq = NewInMemoryBuildQueue(cas, f.clock, func() (uuid.UUID, error) {
		f.lastUUID++
		return uuid.Parse(fmt.Sprintf("00000000-0000-0000-0000-%012x", f.lastUUID))
	}, &configuration, 10000, taskLogFuzzRouter{}, allowAll, allowAll, allowAll, allowAll)

	main := util.Must(digest.NewInstanceName("main"))
	require.NoError(t, f.bq.RegisterTokenPool(main, "a", 2))
	require.NoError(t, f.bq.RegisterTokenPool(main, "b", 3))
	// Workers of the other platform create its queue, which is
	// removed again when they disappear.
	require.NoError(t, f.bq.RegisterPredeclaredPlatformQueue(main, platforms[0], nil, 2, 0, []uint32{1, 4}))
	return f
}

func (f *taskLogFuzzer) tracef(format string, args ...any) {
	f.trace = append(f.trace, fmt.Sprintf("%d: ", f.step)+fmt.Sprintf(format, args...))
}

// reportPanic says how to replay a panic of the build queue's checks.
// Such a panic leaves the build queue locked, so it ends the tests.
func (f *taskLogFuzzer) reportPanic() {
	if r := recover(); r != nil {
		fmt.Fprintf(
			os.Stderr,
			"Seed %d, step %d, task log: %t, repeatable: %t. Replay with -task_log_fuzz_first_seed=%d -task_log_fuzz_seeds=1 -task_log_fuzz_steps=%d. Last calls:\n%s\n",
			f.seed, f.step, f.taskLog, f.repeatable, f.seed, f.step, strings.Join(f.trace[max(len(f.trace)-40, 0):], "\n"),
		)
		panic(r)
	}
}

// isWaiting returns whether a Synchronize() call of a worker waits for
// work.
func (f *taskLogFuzzer) isWaiting(w *taskLogFuzzWorker) bool {
	f.bq.lock.Lock()
	defer f.bq.lock.Unlock()
	for _, scq := range f.bq.sizeClassQueues {
		if bw, ok := scq.workers[newWorkerKey(map[string]string{"hostname": w.name})]; ok {
			return bw.wakeup != nil
		}
	}
	return false
}

// operations returns the stage of the task of every operation.
func (f *taskLogFuzzer) operations() map[string]string {
	f.bq.lock.Lock()
	defer f.bq.lock.Unlock()
	operations := map[string]string{}
	for name, o := range f.bq.operationsNameMap {
		switch t := o.task; {
		case t.executeResponse != nil:
			operations[name] = "completed"
		case t.currentWorker != nil:
			operations[name] = "executing on " + string(t.currentWorker.workerKey)
		case t.blockedPool != nil:
			operations[name] = "parked on " + t.blockedPool.key.name
		default:
			operations[name] = "queued"
		}
	}
	return operations
}

// execute calls Execute() and waits for its first update.
func (f *taskLogFuzzer) execute() {
	ctx, cancel := context.WithCancel(f.ctx)
	request, stream, what := newTaskLogFuzzExecute(ctx, f.random)
	c := &taskLogFuzzClient{cancel: cancel, stream: stream, done: make(chan error, 1)}
	go func() {
		defer f.reportPanic()
		c.done <- f.bq.Execute(request, stream)
	}()
	select {
	case update := <-stream.updates:
		c.operation = update.Name
		f.clients = append(f.clients, c)
		f.tracef("%s: operation %s", what, c.operation[30:])
	case err := <-c.done:
		cancel()
		f.tracef("%s: %v", what, err)
	}
}

func (f *taskLogFuzzer) removeClient(c *taskLogFuzzClient) {
	c.cancel()
	f.clients = slices.DeleteFunc(f.clients, func(other *taskLogFuzzClient) bool { return other == c })
}

// synchronize calls Synchronize() and waits until it either returns
// or waits for work. If a call of the worker already waits for work,
// it is made to time out instead.
func (f *taskLogFuzzer) synchronize(w *taskLogFuzzWorker) {
	if w.waiting != nil {
		w.timer <- f.clock.Now()
		f.tracef("%s stops waiting: %s", w.name, w.handleResult(<-w.waiting))
		w.waiting = nil
		return
	}
	if f.repeatable {
		for _, other := range f.workers {
			if other.waiting != nil {
				f.synchronize(other)
			}
		}
	}

	request, what := w.newRequest(f.random)
	waiting := make(chan taskLogFuzzSynchronizeResult, 1)
	go func() {
		defer f.reportPanic()
		response, err := f.bq.Synchronize(f.ctx, request)
		waiting <- taskLogFuzzSynchronizeResult{response, err}
	}()
	for {
		select {
		case result := <-waiting:
			f.tracef("%s synchronizes, %s: %s", w.name, what, w.handleResult(result))
			return
		default:
		}
		if f.isWaiting(w) {
			f.clock.lock.Lock()
			w.waiting, w.timer = waiting, f.clock.lastIdleTimer
			f.clock.lock.Unlock()
			f.tracef("%s synchronizes, %s: waits for work", w.name, what)
			return
		}
		runtime.Gosched()
	}
}

func (f *taskLogFuzzer) call() {
	defer f.reportPanic()
	switch n := f.random.Intn(100); {
	case n < 30:
		f.execute()
	case n < 70:
		f.synchronize(f.workers[f.random.Intn(len(f.workers))])
	case n < 85:
		// Mostly too short for workers and operations to time out.
		d := []time.Duration{500 * time.Millisecond, 5 * time.Second, 11 * time.Second, 31 * time.Second, 61 * time.Second, 200 * time.Second}[f.random.Intn(6)]
		if d > time.Minute && f.random.Intn(4) != 0 {
			d = time.Second
		}
		f.clock.advance(d)
		// Have the build queue run the cleanups that are due.
		_, err := f.bq.ListPlatformQueues(f.ctx, &emptypb.Empty{})
		f.tracef("advance %s: %v", d, err)
	case n < 93:
		if len(f.clients) > 0 {
			c := f.clients[f.random.Intn(len(f.clients))]
			c.cancel()
			f.tracef("cancel the call for operation %s: %v", c.operation[30:], <-c.done)
			f.removeClient(c)
		}
	default:
		if len(f.clients) > 0 {
			c := f.clients[f.random.Intn(len(f.clients))]
			_, err := f.bq.KillOperations(f.ctx, newTaskLogFuzzKillRequest(c.operation))
			f.tracef("kill operation %s: %v", c.operation[30:], err)
		}
	}
}

// settle waits for the calls that the last step has woken up, so that
// the next step finds the build queue in a state that only depends on
// the steps before it.
func (f *taskLogFuzzer) settle() {
	operations := f.operations()
	for _, c := range slices.Clone(f.clients) {
		if stage, ok := operations[c.operation]; !ok || stage == "completed" {
			err := <-c.done
			var last *longrunningpb.Operation
			for len(c.stream.updates) > 0 {
				last = <-c.stream.updates
			}
			var response remoteexecution.ExecuteResponse
			require.NoError(f.t, last.GetResponse().UnmarshalTo(&response))
			f.tracef("operation %s: %v, %s, exit code %d", c.operation[30:], err, status.FromProto(response.Status).Code(), response.Result.GetExitCode())
			f.removeClient(c)
		}
	}
	for _, w := range f.workers {
		// A worker that was woken up either returns, or waits
		// again if its task was taken away before it looked.
		for w.waiting != nil && !f.isWaiting(w) {
			select {
			case result := <-w.waiting:
				f.tracef("%s stops waiting: %s", w.name, w.handleResult(result))
				w.waiting = nil
			default:
				runtime.Gosched()
			}
		}
	}

	state := []string{}
	for name, stage := range f.operations() {
		state = append(state, name[30:]+" "+stage)
	}
	for _, w := range f.workers {
		if w.waiting != nil {
			state = append(state, w.name+" waits")
		}
	}
	slices.Sort(state)
	f.tracef("%v", state)
}

// run makes a number of random calls, and returns the trace.
func (f *taskLogFuzzer) run(steps int) []string {
	for f.step = 1; f.step <= steps; f.step++ {
		f.call()
		f.settle()
	}

	// Let the calls that still wait return.
	f.cancel()
	for _, c := range f.clients {
		<-c.done
	}
	for _, w := range f.workers {
		if w.waiting != nil {
			<-w.waiting
		}
	}
	// Replays are kept for as long as their build queue is.
	taskLogReplays.Delete(f.bq)
	return f.trace
}

func requireAllTaskLogEvents(t *testing.T, events map[string]int) {
	t.Logf("Lines: %v", events)
	for _, ev := range []string{"start", "queued", "attached", "parked", "executing", "finished"} {
		require.NotZero(t, events[ev], ev)
	}
}

// TestInMemoryBuildQueueTaskLogRandomized makes random calls to a
// build queue that writes a task log. It then makes calls of which the
// outcome does not depend on chance to a build queue without and one
// with a task log, and requires both to answer and end up the same
// after every call, as scheduling must not depend on the task log.
//
// Run more with -task_log_fuzz_seeds and -task_log_fuzz_steps.
func TestInMemoryBuildQueueTaskLogRandomized(t *testing.T) {
	events := map[string]int{}
	for seed := *taskLogFuzzFirstSeed; seed < *taskLogFuzzFirstSeed+*taskLogFuzzSeeds; seed++ {
		f := newTaskLogFuzzer(t, seed, true, false)
		f.run(*taskLogFuzzSteps)
		for ev, n := range f.events {
			events[ev] += n
		}

		without := newTaskLogFuzzer(t, seed, false, true).run(*taskLogFuzzSteps)
		with := newTaskLogFuzzer(t, seed, true, true).run(*taskLogFuzzSteps)
		if !slices.Equal(without, with) {
			i := 0
			for i < min(len(without), len(with)) && without[i] == with[i] {
				i++
			}
			t.Fatalf(
				"Seed %d: the task log changes what the build queue does. Replay with -task_log_fuzz_first_seed=%d -task_log_fuzz_seeds=1.\nWithout task log:\n%s\nWith task log:\n%s",
				seed, seed, strings.Join(without[max(i-8, 0):min(i+1, len(without))], "\n"), strings.Join(with[max(i-8, 0):min(i+1, len(with))], "\n"),
			)
		}
	}
	requireAllTaskLogEvents(t, events)
}

// TestInMemoryBuildQueueTaskLogConcurrent lets clients and workers call
// a build queue at the same time, while its clock runs fast enough for
// workers and operations to time out.
func TestInMemoryBuildQueueTaskLogConcurrent(t *testing.T) {
	f := newTaskLogFuzzer(t, 1, true, false)
	ctx, cancel := context.WithTimeout(f.ctx, *taskLogFuzzConcurrent)
	defer cancel()
	var wg sync.WaitGroup
	wg.Go(func() {
		for ctx.Err() == nil {
			f.clock.advance(time.Second)
			time.Sleep(200 * time.Microsecond)
		}
	})
	for i, w := range f.workers {
		wg.Go(func() {
			random := rand.New(rand.NewSource(int64(i)))
			for ctx.Err() == nil {
				if random.Intn(50) == 0 {
					// Long enough to be taken for gone.
					time.Sleep(20 * time.Millisecond)
				}
				request, _ := w.newRequest(random)
				callCtx, callCancel := context.WithTimeout(ctx, 5*time.Millisecond)
				response, err := f.bq.Synchronize(callCtx, request)
				callCancel()
				w.handleResult(taskLogFuzzSynchronizeResult{response, err})
			}
		})
	}
	for i := 0; i < 8; i++ {
		wg.Go(func() {
			random := rand.New(rand.NewSource(int64(100 + i)))
			for ctx.Err() == nil {
				callCtx, callCancel := context.WithTimeout(ctx, time.Duration(random.Intn(20000))*time.Microsecond)
				request, stream, _ := newTaskLogFuzzExecute(callCtx, random)
				f.bq.Execute(request, stream)
				callCancel()
				if random.Intn(10) == 0 && len(stream.updates) > 0 {
					f.bq.KillOperations(ctx, newTaskLogFuzzKillRequest((<-stream.updates).Name))
				}
			}
		})
	}
	wg.Wait()
	f.cancel()
	requireAllTaskLogEvents(t, f.events)
}
