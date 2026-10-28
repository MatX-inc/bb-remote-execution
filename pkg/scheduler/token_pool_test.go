package scheduler_test

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"testing"
	"time"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-remote-execution/internal/mock"
	"github.com/buildbarn/bb-remote-execution/pkg/proto/buildqueuestate"
	"github.com/buildbarn/bb-remote-execution/pkg/proto/remoteworker"
	"github.com/buildbarn/bb-remote-execution/pkg/scheduler"
	"github.com/buildbarn/bb-remote-execution/pkg/scheduler/invocation"
	"github.com/buildbarn/bb-remote-execution/pkg/scheduler/platform"
	"github.com/buildbarn/bb-storage/pkg/blobstore/buffer"
	"github.com/buildbarn/bb-storage/pkg/clock"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/testutil"
	"github.com/buildbarn/bb-storage/pkg/util"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/emptypb"

	"cloud.google.com/go/longrunning/autogen/longrunningpb"

	"go.uber.org/mock/gomock"
)

// The token pool tests below share a small harness. Unlike the older
// tests in this package, the harness does not script every clock
// access: the mock clock returns a time the test advances explicitly,
// and timers handed to idle workers fire immediately, so that a
// Synchronize() call for which no work is assignable returns an idle
// response instead of blocking. Timers handed to clients never fire.

const tokenIdleWorkerSynchronizationInterval = 30 * time.Second

var tokenBuildQueueConfigurationForTesting = scheduler.InMemoryBuildQueueConfiguration{
	ExecutionUpdateInterval:              time.Minute,
	OperationWithNoWaitersTimeout:        time.Minute,
	PlatformQueueWithNoWorkersTimeout:    15 * time.Minute,
	BusyWorkerSynchronizationInterval:    10 * time.Second,
	GetIdleWorkerSynchronizationInterval: func() time.Duration { return tokenIdleWorkerSynchronizationInterval },
	WorkerTaskRetryCount:                 9,
	WorkerWithNoSynchronizationsTimeout:  time.Minute,
}

// otherPlatformForTesting is a second platform under the same instance
// name prefix, used to show that token pools span platform queues.
var otherPlatformForTesting = &remoteexecution.Platform{
	Properties: []*remoteexecution.Platform_Property{
		{Name: "cpu", Value: "x86_64"},
		{Name: "os", Value: "linux"},
	},
}

type fakeTimer struct{}

func (fakeTimer) Stop() bool { return true }

type tokenTestEnv struct {
	t      *testing.T
	ctrl   *gomock.Controller
	ctx    context.Context
	cas    *mock.MockBlobAccess
	uuid   *mock.MockUUIDGenerator
	router *mock.MockActionRouter
	bq     *scheduler.InMemoryBuildQueue
	client remoteexecution.ExecutionClient

	lock sync.Mutex
	now  time.Time
}

func newTokenTestEnv(t *testing.T, configuration *scheduler.InMemoryBuildQueueConfiguration, start time.Time) *tokenTestEnv {
	ctrl, ctx := gomock.WithContext(context.Background(), t)
	env := &tokenTestEnv{
		t:      t,
		ctrl:   ctrl,
		ctx:    ctx,
		cas:    mock.NewMockBlobAccess(ctrl),
		uuid:   mock.NewMockUUIDGenerator(ctrl),
		router: mock.NewMockActionRouter(ctrl),
		now:    start,
	}
	clk := mock.NewMockClock(ctrl)
	clk.EXPECT().Now().DoAndReturn(env.getNow).AnyTimes()
	clk.EXPECT().NewTimer(gomock.Any()).DoAndReturn(func(d time.Duration) (clock.Timer, <-chan time.Time) {
		// Not a gomock timer: a server goroutine woken as a test
		// returns may create one while the controller verifies
		// expectations at cleanup.
		var timer fakeTimer
		if d == tokenIdleWorkerSynchronizationInterval {
			ch := make(chan time.Time, 1)
			ch <- env.getNow()
			return timer, ch
		}
		return timer, nil
	}).AnyTimes()
	env.bq = scheduler.NewInMemoryBuildQueue(env.cas, clk, env.uuid.Call, configuration, 10000, env.router, allowAllAuthorizer, allowAllAuthorizer, allowAllAuthorizer, allowAllAuthorizer)
	env.client = getExecutionClient(t, env.bq)
	return env
}

func (env *tokenTestEnv) getNow() time.Time {
	env.lock.Lock()
	defer env.lock.Unlock()
	return env.now
}

func (env *tokenTestEnv) advance(d time.Duration) {
	env.lock.Lock()
	defer env.lock.Unlock()
	env.now = env.now.Add(d)
}

func (env *tokenTestEnv) registerPool(name string, capacity uint32) {
	require.NoError(env.t, env.bq.RegisterTokenPool(util.Must(digest.NewInstanceName("main")), name, capacity))
}

func (env *tokenTestEnv) registerPlatformQueue(platform *remoteexecution.Platform, maximumQueuedBackgroundLearningOperations int, sizeClasses []uint32) {
	require.NoError(env.t, env.bq.RegisterPredeclaredPlatformQueue(
		util.Must(digest.NewInstanceName("main")),
		platform,
		/* workerInvocationStickinessLimits = */ nil,
		maximumQueuedBackgroundLearningOperations,
		/* backgroundLearningOperationPriority = */ 0,
		sizeClasses,
	))
}

// tokenActionHash returns a distinct MD5-sized action hash.
func tokenActionHash(n int) string {
	return fmt.Sprintf("%032x", n)
}

func tokenOperationName(n int) string {
	return fmt.Sprintf("00000000-0000-0000-0000-%012x", n)
}

// executeOptions describes one Execute() call made through the harness.
type executeOptions struct {
	hash          string
	operationName string
	// Token requirements, added to the base platform as
	// "token:<name>" properties.
	tokens map[string]uint32
	// The platform the router will assign the action to. Defaults
	// to platformForTesting.
	platform *remoteexecution.Platform
	// When set, the router returns an invocation key for this ID.
	invocationID string
	// Size classes the platform queue offers and the index the
	// selector picks. Default: [0] and 0.
	sizeClasses    []uint32
	sizeClassIndex int
	// Learner returned by the selector. When nil, a learner that
	// tolerates any outcome is created.
	learner *mock.MockLearner
	// When set, the selector is expected to be abandoned instead of
	// asked to select a size class.
	selectorAbandoned bool
	// When set, no routing is expected to take place.
	noRouting bool
}

func (tokenTestEnv) buildAction(o *executeOptions) (*remoteexecution.Action, *remoteexecution.Action) {
	basePlatform := o.platform
	if basePlatform == nil {
		basePlatform = platformForTesting
	}
	tokenNames := make([]string, 0, len(o.tokens))
	for name := range o.tokens {
		tokenNames = append(tokenNames, name)
	}
	sort.Strings(tokenNames)
	properties := append([]*remoteexecution.Platform_Property(nil), basePlatform.Properties...)
	for _, name := range tokenNames {
		properties = append(properties, &remoteexecution.Platform_Property{
			Name:  "token:" + name,
			Value: fmt.Sprintf("%d", o.tokens[name]),
		})
	}
	commandDigest := &remoteexecution.Digest{
		Hash:      "61c585c297d00409bd477b6b80759c94ec545ab4",
		SizeBytes: 456,
	}
	action := &remoteexecution.Action{
		CommandDigest: commandDigest,
		Platform:      &remoteexecution.Platform{Properties: properties},
	}
	strippedAction := &remoteexecution.Action{
		CommandDigest: commandDigest,
		Platform:      &remoteexecution.Platform{Properties: basePlatform.Properties},
	}
	return action, strippedAction
}

// expectExecute installs the mock expectations for one Execute() call
// and returns the learner the selector will hand out.
func (env *tokenTestEnv) expectExecute(o *executeOptions, action, strippedAction *remoteexecution.Action) *mock.MockLearner {
	t := env.t
	env.cas.EXPECT().Get(
		gomock.Any(),
		digest.MustNewDigest("main", remoteexecution.DigestFunction_MD5, o.hash, 123),
	).Return(buffer.NewProtoBufferFromProto(action, buffer.UserProvided))
	if o.noRouting {
		return nil
	}

	basePlatform := o.platform
	if basePlatform == nil {
		basePlatform = platformForTesting
	}
	var invocationKeys []invocation.Key
	if o.invocationID != "" {
		requestMetadataAny, err := anypb.New(&remoteexecution.RequestMetadata{
			ToolInvocationId: o.invocationID,
		})
		require.NoError(t, err)
		invocationKeys = []invocation.Key{util.Must(invocation.NewKey(requestMetadataAny))}
	}
	initialSizeClassSelector := mock.NewMockSelector(env.ctrl)
	// The router must observe the action with its token properties
	// removed, so that the platform key it computes matches the
	// workers.
	env.router.EXPECT().RouteAction(gomock.Any(), gomock.Any(), testutil.EqProto(t, strippedAction), gomock.Any()).Return(
		strippedAction,
		platform.MustNewKey("main", basePlatform),
		invocationKeys,
		initialSizeClassSelector,
		nil,
	)
	if o.selectorAbandoned {
		initialSizeClassSelector.EXPECT().Abandoned()
		if o.operationName != "" {
			// In-flight deduplication still creates an
			// operation.
			env.uuid.EXPECT().Call().Return(uuid.Parse(o.operationName))
		}
		return nil
	}

	sizeClasses := o.sizeClasses
	if sizeClasses == nil {
		sizeClasses = []uint32{0}
	}
	learner := o.learner
	if learner == nil {
		learner = mock.NewMockLearner(env.ctrl)
		learner.EXPECT().Abandoned().AnyTimes()
		learner.EXPECT().Succeeded(gomock.Any(), gomock.Any()).Return(0, time.Duration(0), time.Duration(0), nil).AnyTimes()
	}
	initialSizeClassSelector.EXPECT().Select(sizeClasses).
		Return(o.sizeClassIndex, 15*time.Minute, 30*time.Minute, learner)
	env.uuid.EXPECT().Call().Return(uuid.Parse(o.operationName))
	return learner
}

// execute performs an Execute() call and waits for the initial QUEUED
// update.
func (env *tokenTestEnv) execute(o *executeOptions) remoteexecution.Execution_ExecuteClient {
	stream, err := env.executeStream(env.ctx, o)
	require.NoError(env.t, err)
	requireOperationStage(env.t, stream, o.operationName, o.hash, remoteexecution.ExecutionStage_QUEUED)
	return stream
}

// executeExpectingError performs an Execute() call that is expected to
// fail before an operation is created.
func (env *tokenTestEnv) executeExpectingError(o *executeOptions) error {
	stream, err := env.executeStream(env.ctx, o)
	require.NoError(env.t, err)
	_, err = stream.Recv()
	return err
}

func (env *tokenTestEnv) executeStream(ctx context.Context, o *executeOptions) (remoteexecution.Execution_ExecuteClient, error) {
	action, strippedAction := env.buildAction(o)
	env.expectExecute(o, action, strippedAction)
	return env.client.Execute(ctx, &remoteexecution.ExecuteRequest{
		InstanceName: "main",
		ActionDigest: &remoteexecution.Digest{
			Hash:      o.hash,
			SizeBytes: 123,
		},
	})
}

func requireOperationStage(t *testing.T, stream remoteexecution.Execution_ExecuteClient, operationName, hash string, stage remoteexecution.ExecutionStage_Value) *remoteexecution.ExecuteResponse {
	update, err := stream.Recv()
	require.NoError(t, err)
	require.Equal(t, operationName, update.Name)
	var metadata remoteexecution.ExecuteOperationMetadata
	require.NoError(t, update.Metadata.UnmarshalTo(&metadata))
	require.Equal(t, stage, metadata.Stage)
	require.Equal(t, hash, metadata.ActionDigest.Hash)
	if stage != remoteexecution.ExecutionStage_COMPLETED {
		require.False(t, update.Done)
		return nil
	}
	require.True(t, update.Done)
	var executeResponse remoteexecution.ExecuteResponse
	require.NoError(t, update.Result.(*longrunningpb.Operation_Response).Response.UnmarshalTo(&executeResponse))
	return &executeResponse
}

func tokenWorkerID(name string) map[string]string {
	return map[string]string{"hostname": name}
}

func idleWorkerState() *remoteworker.CurrentState {
	return &remoteworker.CurrentState{
		WorkerState: &remoteworker.CurrentState_Idle{
			Idle: &emptypb.Empty{},
		},
	}
}

func executingWorkerState(hash string) *remoteworker.CurrentState {
	return &remoteworker.CurrentState{
		WorkerState: &remoteworker.CurrentState_Executing_{
			Executing: &remoteworker.CurrentState_Executing{
				ActionDigest: &remoteexecution.Digest{
					Hash:      hash,
					SizeBytes: 123,
				},
				ExecutionState: &remoteworker.CurrentState_Executing_FetchingInputs{
					FetchingInputs: &emptypb.Empty{},
				},
			},
		},
	}
}

func completedWorkerState(hash string, executeResponse *remoteexecution.ExecuteResponse) *remoteworker.CurrentState {
	return &remoteworker.CurrentState{
		WorkerState: &remoteworker.CurrentState_Executing_{
			Executing: &remoteworker.CurrentState_Executing{
				ActionDigest: &remoteexecution.Digest{
					Hash:      hash,
					SizeBytes: 123,
				},
				ExecutionState: &remoteworker.CurrentState_Executing_Completed{
					Completed: executeResponse,
				},
			},
		},
	}
}

func successfulExecuteResponse() *remoteexecution.ExecuteResponse {
	return &remoteexecution.ExecuteResponse{
		Result: &remoteexecution.ActionResult{
			ExecutionMetadata: &remoteexecution.ExecutedActionMetadata{},
		},
	}
}

func (env *tokenTestEnv) synchronize(worker string, platform *remoteexecution.Platform, sizeClass uint32, state *remoteworker.CurrentState) *remoteworker.SynchronizeResponse {
	return env.synchronizeWithPreference(worker, platform, sizeClass, state, false)
}

func (env *tokenTestEnv) synchronizeWithPreference(worker string, platform *remoteexecution.Platform, sizeClass uint32, state *remoteworker.CurrentState, preferBeingIdle bool) *remoteworker.SynchronizeResponse {
	response, err := env.bq.Synchronize(env.ctx, &remoteworker.SynchronizeRequest{
		WorkerId:           tokenWorkerID(worker),
		InstanceNamePrefix: "main",
		Platform:           platform,
		SizeClass:          sizeClass,
		CurrentState:       state,
		PreferBeingIdle:    preferBeingIdle,
	})
	require.NoError(env.t, err)
	return response
}

func requireIdle(t *testing.T, response *remoteworker.SynchronizeResponse) {
	_, ok := response.DesiredState.GetWorkerState().(*remoteworker.DesiredState_Idle)
	require.True(t, ok, "expected worker to be idle, got %v", response.DesiredState)
}

func requireExecuting(t *testing.T, response *remoteworker.SynchronizeResponse, hash string) *remoteworker.DesiredState_Executing {
	executing, ok := response.DesiredState.GetWorkerState().(*remoteworker.DesiredState_Executing_)
	require.True(t, ok, "expected worker to execute %s, got %v", hash, response.DesiredState)
	require.Equal(t, hash, executing.Executing.ActionDigest.Hash)
	return executing.Executing
}

func (env *tokenTestEnv) pool(name string) *buildqueuestate.TokenPoolState {
	response, err := env.bq.ListPlatformQueues(env.ctx, &emptypb.Empty{})
	require.NoError(env.t, err)
	for _, p := range response.TokenPools {
		if p.Name == name {
			return p
		}
	}
	require.Fail(env.t, "token pool not found", name)
	return nil
}

func (env *tokenTestEnv) requirePool(name string, inUse, blocked uint32) {
	p := env.pool(name)
	require.Equal(env.t, inUse, p.InUse, "in use of pool %s", name)
	require.Equal(env.t, blocked, p.BlockedTasksCount, "blocked tasks of pool %s", name)
}

func (env *tokenTestEnv) operation(name string) *buildqueuestate.OperationState {
	response, err := env.bq.GetOperation(env.ctx, &buildqueuestate.GetOperationRequest{
		OperationName: name,
	})
	require.NoError(env.t, err)
	return response.Operation
}

func (env *tokenTestEnv) listOperations(stage remoteexecution.ExecutionStage_Value, token string) []string {
	return env.listOperationsUnderPrefix(stage, "main", token, false)
}

func (env *tokenTestEnv) listOperationsUnderPrefix(stage remoteexecution.ExecutionStage_Value, instanceNamePrefix, token string, blockedOnly bool) []string {
	response, err := env.bq.ListOperations(env.ctx, &buildqueuestate.ListOperationsRequest{
		PageSize:                      100,
		FilterStage:                   stage,
		FilterTokenName:               token,
		FilterTokenInstanceNamePrefix: instanceNamePrefix,
		FilterTokenBlockedOnly:        blockedOnly,
	})
	require.NoError(env.t, err)
	names := make([]string, 0, len(response.Operations))
	for _, o := range response.Operations {
		names = append(names, o.Name)
	}
	return names
}

// Token properties are parsed and removed before routing. The router
// and the worker see the action without them, while the action digest
// presented to the worker is the client's.
func TestInMemoryBuildQueueTokenPoolsStripAndRoute(t *testing.T) {
	env := newTokenTestEnv(t, &tokenBuildQueueConfigurationForTesting, time.Unix(1000, 0))
	env.registerPool("oranges", 4)
	requireIdle(t, env.synchronize("worker1", platformForTesting, 0, idleWorkerState()))

	env.execute(&executeOptions{
		hash:          tokenActionHash(1),
		operationName: tokenOperationName(1),
		tokens:        map[string]uint32{"oranges": 2},
	})
	o := env.operation(tokenOperationName(1))
	require.Len(t, o.TokenRequirements, 1)
	testutil.RequireEqualProto(t, &buildqueuestate.TokenRequirement{Name: "oranges", Amount: 2}, o.TokenRequirements[0])
	require.Empty(t, o.BlockedOnToken)

	executing := requireExecuting(t, env.synchronize("worker1", platformForTesting, 0, idleWorkerState()), tokenActionHash(1))
	testutil.RequireEqualProto(t, platformForTesting, executing.Action.Platform)
	env.requirePool("oranges", 2, 0)

	t.Run("ZeroAmount", func(t *testing.T) {
		err := env.executeExpectingError(&executeOptions{
			hash:      tokenActionHash(2),
			tokens:    map[string]uint32{"oranges": 0},
			noRouting: true,
		})
		testutil.RequireEqualStatus(t, status.Error(codes.InvalidArgument, "Token \"oranges\" has amount \"0\", which is not an integer in [1, 4294967295]"), err)
	})

	t.Run("NonNumericAmount", func(t *testing.T) {
		action := &remoteexecution.Action{
			Platform: &remoteexecution.Platform{
				Properties: []*remoteexecution.Platform_Property{
					{Name: "token:oranges", Value: "many"},
				},
			},
		}
		env.cas.EXPECT().Get(
			gomock.Any(),
			digest.MustNewDigest("main", remoteexecution.DigestFunction_MD5, tokenActionHash(3), 123),
		).Return(buffer.NewProtoBufferFromProto(action, buffer.UserProvided))
		stream, err := env.client.Execute(env.ctx, &remoteexecution.ExecuteRequest{
			InstanceName: "main",
			ActionDigest: &remoteexecution.Digest{Hash: tokenActionHash(3), SizeBytes: 123},
		})
		require.NoError(t, err)
		_, err = stream.Recv()
		testutil.RequireEqualStatus(t, status.Error(codes.InvalidArgument, "Token \"oranges\" has amount \"many\", which is not an integer in [1, 4294967295]"), err)
	})

	t.Run("DuplicateToken", func(t *testing.T) {
		action := &remoteexecution.Action{
			Platform: &remoteexecution.Platform{
				Properties: []*remoteexecution.Platform_Property{
					{Name: "token:oranges", Value: "1"},
					{Name: "token:oranges", Value: "2"},
				},
			},
		}
		env.cas.EXPECT().Get(
			gomock.Any(),
			digest.MustNewDigest("main", remoteexecution.DigestFunction_MD5, tokenActionHash(4), 123),
		).Return(buffer.NewProtoBufferFromProto(action, buffer.UserProvided))
		stream, err := env.client.Execute(env.ctx, &remoteexecution.ExecuteRequest{
			InstanceName: "main",
			ActionDigest: &remoteexecution.Digest{Hash: tokenActionHash(4), SizeBytes: 123},
		})
		require.NoError(t, err)
		_, err = stream.Recv()
		testutil.RequireEqualStatus(t, status.Error(codes.InvalidArgument, "Token \"oranges\" is required more than once"), err)
	})

	t.Run("EmptyName", func(t *testing.T) {
		action := &remoteexecution.Action{
			Platform: &remoteexecution.Platform{
				Properties: []*remoteexecution.Platform_Property{
					{Name: "token:", Value: "1"},
				},
			},
		}
		env.cas.EXPECT().Get(
			gomock.Any(),
			digest.MustNewDigest("main", remoteexecution.DigestFunction_MD5, tokenActionHash(5), 123),
		).Return(buffer.NewProtoBufferFromProto(action, buffer.UserProvided))
		stream, err := env.client.Execute(env.ctx, &remoteexecution.ExecuteRequest{
			InstanceName: "main",
			ActionDigest: &remoteexecution.Digest{Hash: tokenActionHash(5), SizeBytes: 123},
		})
		require.NoError(t, err)
		_, err = stream.Recv()
		testutil.RequireEqualStatus(t, status.Error(codes.InvalidArgument, "Platform property \"token:\" does not name a token"), err)
	})
}

// Requirements are resolved against the pools of the platform queue's
// instance name prefix. Unknown pools and requirements beyond a pool's
// capacity fail fast and abandon the size class selector.
func TestInMemoryBuildQueueTokenPoolsUnknownOrOversized(t *testing.T) {
	env := newTokenTestEnv(t, &tokenBuildQueueConfigurationForTesting, time.Unix(1000, 0))
	env.registerPool("oranges", 2)
	env.registerPool("apples", 0)
	requireIdle(t, env.synchronize("worker1", platformForTesting, 0, idleWorkerState()))

	err := env.executeExpectingError(&executeOptions{
		hash:              tokenActionHash(1),
		tokens:            map[string]uint32{"pears": 1},
		selectorAbandoned: true,
	})
	testutil.RequireEqualStatus(t, status.Error(codes.FailedPrecondition, "No token pool named \"pears\" exists for instance name prefix \"main\""), err)

	err = env.executeExpectingError(&executeOptions{
		hash:              tokenActionHash(2),
		tokens:            map[string]uint32{"oranges": 3},
		selectorAbandoned: true,
	})
	testutil.RequireEqualStatus(t, status.Error(codes.FailedPrecondition, "Action requires 3 tokens of pool \"oranges\" for instance name prefix \"main\", which exceeds its capacity of 2"), err)

	// A pool with capacity zero rejects every requirement.
	err = env.executeExpectingError(&executeOptions{
		hash:              tokenActionHash(3),
		tokens:            map[string]uint32{"apples": 1},
		selectorAbandoned: true,
	})
	testutil.RequireEqualStatus(t, status.Error(codes.FailedPrecondition, "Action requires 1 tokens of pool \"apples\" for instance name prefix \"main\", which exceeds its capacity of 0"), err)

	require.Empty(t, env.listOperations(remoteexecution.ExecutionStage_UNKNOWN, ""))
}

// A second task requiring a fully used pool is parked and proceeds
// once the first one releases its tokens.
func TestInMemoryBuildQueueTokenPoolsCapacityGate(t *testing.T) {
	env := newTokenTestEnv(t, &tokenBuildQueueConfigurationForTesting, time.Unix(1000, 0))
	env.registerPool("oranges", 1)
	requireIdle(t, env.synchronize("worker1", platformForTesting, 0, idleWorkerState()))

	stream1 := env.execute(&executeOptions{
		hash:          tokenActionHash(1),
		operationName: tokenOperationName(1),
		tokens:        map[string]uint32{"oranges": 1},
	})
	requireExecuting(t, env.synchronize("worker1", platformForTesting, 0, idleWorkerState()), tokenActionHash(1))
	requireOperationStage(t, stream1, tokenOperationName(1), tokenActionHash(1), remoteexecution.ExecutionStage_EXECUTING)
	env.requirePool("oranges", 1, 0)

	env.advance(time.Second)
	stream2 := env.execute(&executeOptions{
		hash:          tokenActionHash(2),
		operationName: tokenOperationName(2),
		tokens:        map[string]uint32{"oranges": 1},
	})
	env.requirePool("oranges", 1, 1)
	require.Equal(t, "oranges", env.operation(tokenOperationName(2)).BlockedOnToken)

	// A second worker finds nothing to do, even though a task is
	// in the QUEUED stage.
	requireIdle(t, env.synchronize("worker2", platformForTesting, 0, idleWorkerState()))

	// Completing the first task releases the token. The same
	// worker picks up the second task.
	env.advance(time.Second)
	requireExecuting(t, env.synchronize("worker1", platformForTesting, 0, completedWorkerState(tokenActionHash(1), successfulExecuteResponse())), tokenActionHash(2))
	requireOperationStage(t, stream1, tokenOperationName(1), tokenActionHash(1), remoteexecution.ExecutionStage_COMPLETED)
	requireOperationStage(t, stream2, tokenOperationName(2), tokenActionHash(2), remoteexecution.ExecutionStage_EXECUTING)
	env.requirePool("oranges", 1, 0)
	require.Empty(t, env.operation(tokenOperationName(2)).BlockedOnToken)
}

// A parked task at the head of the queue does not hold up token-free
// work behind it: the worker parks it and continues its search.
func TestInMemoryBuildQueueTokenPoolsTokenFreeWorkNotStarved(t *testing.T) {
	env := newTokenTestEnv(t, &tokenBuildQueueConfigurationForTesting, time.Unix(1000, 0))
	env.registerPool("oranges", 1)
	env.registerPlatformQueue(platformForTesting, 0, []uint32{0})

	// Queue two token-requiring tasks and a token-free one while
	// no workers exist. Both token-requiring tasks are eligible at
	// this point, so none of them is parked yet.
	streams := make([]remoteexecution.Execution_ExecuteClient, 0, 3)
	for i, tokens := range []map[string]uint32{
		{"oranges": 1},
		{"oranges": 1},
		nil,
	} {
		env.advance(time.Second)
		streams = append(streams, env.execute(&executeOptions{
			hash:          tokenActionHash(i),
			operationName: tokenOperationName(i),
			tokens:        tokens,
		}))
	}
	env.requirePool("oranges", 0, 0)

	// The first worker takes the first task. The second worker
	// skips the second task, as the pool is now exhausted, and
	// takes the token-free third one.
	requireExecuting(t, env.synchronize("worker1", platformForTesting, 0, idleWorkerState()), tokenActionHash(0))
	requireOperationStage(t, streams[0], tokenOperationName(0), tokenActionHash(0), remoteexecution.ExecutionStage_EXECUTING)
	requireExecuting(t, env.synchronize("worker2", platformForTesting, 0, idleWorkerState()), tokenActionHash(2))
	requireOperationStage(t, streams[2], tokenOperationName(2), tokenActionHash(2), remoteexecution.ExecutionStage_EXECUTING)
	env.requirePool("oranges", 1, 1)
	require.Equal(t, "oranges", env.operation(tokenOperationName(1)).BlockedOnToken)

	// Parked operations are no longer reported as queued in the
	// invocation, but the invocation remains active.
	response, err := env.bq.ListPlatformQueues(env.ctx, &emptypb.Empty{})
	require.NoError(t, err)
	root := response.PlatformQueues[0].SizeClassQueues[0].RootInvocation
	require.Equal(t, uint32(0), root.QueuedOperationsCount.Direct)
	require.Equal(t, uint32(1), root.BlockedOperationsCount)
	require.Equal(t, uint32(2), root.ExecutingWorkersCount)
}

// A request that the pool cannot satisfy holds no tokens and does not
// delay a later, smaller request that fits.
func TestInMemoryBuildQueueTokenPoolsSmallerRequestPassesLarger(t *testing.T) {
	env := newTokenTestEnv(t, &tokenBuildQueueConfigurationForTesting, time.Unix(1000, 0))
	env.registerPool("oranges", 4)
	requireIdle(t, env.synchronize("worker1", platformForTesting, 0, idleWorkerState()))

	stream1 := env.execute(&executeOptions{
		hash:          tokenActionHash(1),
		operationName: tokenOperationName(1),
		tokens:        map[string]uint32{"oranges": 3},
	})
	requireExecuting(t, env.synchronize("worker1", platformForTesting, 0, idleWorkerState()), tokenActionHash(1))
	requireOperationStage(t, stream1, tokenOperationName(1), tokenActionHash(1), remoteexecution.ExecutionStage_EXECUTING)

	// A request for four tokens is parked. A request for a single
	// token fits next to the running task and is not held behind it.
	env.advance(time.Second)
	stream2 := env.execute(&executeOptions{
		hash:          tokenActionHash(2),
		operationName: tokenOperationName(2),
		tokens:        map[string]uint32{"oranges": 4},
	})
	env.advance(time.Second)
	stream3 := env.execute(&executeOptions{
		hash:          tokenActionHash(3),
		operationName: tokenOperationName(3),
		tokens:        map[string]uint32{"oranges": 1},
	})
	requireExecuting(t, env.synchronize("worker2", platformForTesting, 0, idleWorkerState()), tokenActionHash(3))
	requireOperationStage(t, stream3, tokenOperationName(3), tokenActionHash(3), remoteexecution.ExecutionStage_EXECUTING)
	env.requirePool("oranges", 4, 1)

	// Releasing three tokens is not enough for the four-token
	// request while the single token is held.
	env.advance(time.Second)
	requireIdle(t, env.synchronize("worker1", platformForTesting, 0, completedWorkerState(tokenActionHash(1), successfulExecuteResponse())))
	requireOperationStage(t, stream1, tokenOperationName(1), tokenActionHash(1), remoteexecution.ExecutionStage_COMPLETED)
	env.requirePool("oranges", 1, 1)

	env.advance(time.Second)
	requireExecuting(t, env.synchronize("worker2", platformForTesting, 0, completedWorkerState(tokenActionHash(3), successfulExecuteResponse())), tokenActionHash(2))
	requireOperationStage(t, stream3, tokenOperationName(3), tokenActionHash(3), remoteexecution.ExecutionStage_COMPLETED)
	requireOperationStage(t, stream2, tokenOperationName(2), tokenActionHash(2), remoteexecution.ExecutionStage_EXECUTING)
	env.requirePool("oranges", 4, 0)
}

// A task needing an exhausted pool does not hold back another pool it
// also needs: tasks needing only that other pool keep taking its
// tokens, and the waiting task runs once all of its pools have room.
func TestInMemoryBuildQueueTokenPoolsExhaustedPoolDoesNotBlockOthers(t *testing.T) {
	env := newTokenTestEnv(t, &tokenBuildQueueConfigurationForTesting, time.Unix(1000, 0))
	env.registerPool("oranges", 2)
	env.registerPool("apples", 1)
	requireIdle(t, env.synchronize("worker1", platformForTesting, 0, idleWorkerState()))

	env.execute(&executeOptions{
		hash:          tokenActionHash(1),
		operationName: tokenOperationName(1),
		tokens:        map[string]uint32{"apples": 1},
	})
	requireExecuting(t, env.synchronize("worker1", platformForTesting, 0, idleWorkerState()), tokenActionHash(1))

	// Two tasks needing both pools wait for "apples".
	for i := 2; i <= 3; i++ {
		env.advance(time.Second)
		env.execute(&executeOptions{
			hash:          tokenActionHash(i),
			operationName: tokenOperationName(i),
			tokens:        map[string]uint32{"oranges": 1, "apples": 1},
		})
		require.Equal(t, "apples", env.operation(tokenOperationName(i)).BlockedOnToken)
	}

	// Tasks needing only "oranges" take both of its tokens.
	for i, worker := range []string{"worker2", "worker3"} {
		env.advance(time.Second)
		env.execute(&executeOptions{
			hash:          tokenActionHash(4 + i),
			operationName: tokenOperationName(4 + i),
			tokens:        map[string]uint32{"oranges": 1},
		})
		requireExecuting(t, env.synchronize(worker, platformForTesting, 0, idleWorkerState()), tokenActionHash(4+i))
	}
	env.requirePool("oranges", 2, 0)
	env.requirePool("apples", 1, 2)

	// A further task needing only "oranges" waits for it, and takes
	// the next token it frees, although tasks waiting for "apples"
	// were parked before it.
	env.advance(time.Second)
	env.execute(&executeOptions{
		hash:          tokenActionHash(6),
		operationName: tokenOperationName(6),
		tokens:        map[string]uint32{"oranges": 1},
	})
	require.Equal(t, "oranges", env.operation(tokenOperationName(6)).BlockedOnToken)
	env.advance(time.Second)
	requireExecuting(t, env.synchronize("worker2", platformForTesting, 0, completedWorkerState(tokenActionHash(4), successfulExecuteResponse())), tokenActionHash(6))
	env.requirePool("oranges", 2, 0)
	env.requirePool("apples", 1, 2)

	// Releasing "apples" is not enough while "oranges" is exhausted;
	// the waiting tasks now report "oranges".
	env.advance(time.Second)
	requireIdle(t, env.synchronize("worker1", platformForTesting, 0, completedWorkerState(tokenActionHash(1), successfulExecuteResponse())))
	require.Equal(t, "oranges", env.operation(tokenOperationName(2)).BlockedOnToken)
	env.requirePool("oranges", 2, 2)
	env.requirePool("apples", 0, 0)

	// Releasing a token of "oranges" runs the first of them.
	env.advance(time.Second)
	requireExecuting(t, env.synchronize("worker3", platformForTesting, 0, completedWorkerState(tokenActionHash(5), successfulExecuteResponse())), tokenActionHash(2))
	env.requirePool("oranges", 2, 1)
	env.requirePool("apples", 1, 0)
}

// When one release lets several parked tasks fit, the one parked first
// runs first, whatever its size.
func TestInMemoryBuildQueueTokenPoolsOlderParkedTaskFirst(t *testing.T) {
	env := newTokenTestEnv(t, &tokenBuildQueueConfigurationForTesting, time.Unix(1000, 0))
	env.registerPool("oranges", 4)
	requireIdle(t, env.synchronize("worker1", platformForTesting, 0, idleWorkerState()))

	env.execute(&executeOptions{
		hash:          tokenActionHash(1),
		operationName: tokenOperationName(1),
		tokens:        map[string]uint32{"oranges": 4},
	})
	requireExecuting(t, env.synchronize("worker1", platformForTesting, 0, idleWorkerState()), tokenActionHash(1))
	for i, n := range []uint32{4, 1} {
		env.advance(time.Second)
		env.execute(&executeOptions{
			hash:          tokenActionHash(2 + i),
			operationName: tokenOperationName(2 + i),
			tokens:        map[string]uint32{"oranges": n},
		})
	}
	requireIdle(t, env.synchronize("worker2", platformForTesting, 0, idleWorkerState()))
	env.requirePool("oranges", 4, 2)

	env.advance(time.Second)
	requireExecuting(t, env.synchronize("worker1", platformForTesting, 0, completedWorkerState(tokenActionHash(1), successfulExecuteResponse())), tokenActionHash(2))
	requireIdle(t, env.synchronize("worker2", platformForTesting, 0, idleWorkerState()))
	env.requirePool("oranges", 4, 1)

	env.advance(time.Second)
	requireExecuting(t, env.synchronize("worker1", platformForTesting, 0, completedWorkerState(tokenActionHash(2), successfulExecuteResponse())), tokenActionHash(3))
	env.requirePool("oranges", 1, 0)
}

// A task requiring two pools moves to the FIFO of the pool that
// blocks it, and runs once both pools can satisfy it.
func TestInMemoryBuildQueueTokenPoolsMultipleTokensRepark(t *testing.T) {
	env := newTokenTestEnv(t, &tokenBuildQueueConfigurationForTesting, time.Unix(1000, 0))
	env.registerPool("a", 1)
	env.registerPool("b", 1)
	requireIdle(t, env.synchronize("worker1", platformForTesting, 0, idleWorkerState()))
	requireIdle(t, env.synchronize("worker2", platformForTesting, 0, idleWorkerState()))

	stream1 := env.execute(&executeOptions{
		hash:          tokenActionHash(1),
		operationName: tokenOperationName(1),
		tokens:        map[string]uint32{"a": 1},
	})
	requireExecuting(t, env.synchronize("worker1", platformForTesting, 0, idleWorkerState()), tokenActionHash(1))
	requireOperationStage(t, stream1, tokenOperationName(1), tokenActionHash(1), remoteexecution.ExecutionStage_EXECUTING)
	env.advance(time.Second)
	stream2 := env.execute(&executeOptions{
		hash:          tokenActionHash(2),
		operationName: tokenOperationName(2),
		tokens:        map[string]uint32{"b": 1},
	})
	requireExecuting(t, env.synchronize("worker2", platformForTesting, 0, idleWorkerState()), tokenActionHash(2))
	requireOperationStage(t, stream2, tokenOperationName(2), tokenActionHash(2), remoteexecution.ExecutionStage_EXECUTING)

	env.advance(time.Second)
	stream3 := env.execute(&executeOptions{
		hash:          tokenActionHash(3),
		operationName: tokenOperationName(3),
		tokens:        map[string]uint32{"a": 1, "b": 1},
	})
	require.Equal(t, "a", env.operation(tokenOperationName(3)).BlockedOnToken)
	env.requirePool("a", 1, 1)
	env.requirePool("b", 1, 0)

	// Releasing "a" moves the task to the FIFO of "b".
	env.advance(time.Second)
	requireIdle(t, env.synchronize("worker1", platformForTesting, 0, completedWorkerState(tokenActionHash(1), successfulExecuteResponse())))
	requireOperationStage(t, stream1, tokenOperationName(1), tokenActionHash(1), remoteexecution.ExecutionStage_COMPLETED)
	require.Equal(t, "b", env.operation(tokenOperationName(3)).BlockedOnToken)
	env.requirePool("a", 0, 0)
	env.requirePool("b", 1, 1)

	// Releasing "b" lets it run.
	env.advance(time.Second)
	requireExecuting(t, env.synchronize("worker2", platformForTesting, 0, completedWorkerState(tokenActionHash(2), successfulExecuteResponse())), tokenActionHash(3))
	requireOperationStage(t, stream2, tokenOperationName(2), tokenActionHash(2), remoteexecution.ExecutionStage_COMPLETED)
	requireOperationStage(t, stream3, tokenOperationName(3), tokenActionHash(3), remoteexecution.ExecutionStage_EXECUTING)
	env.requirePool("a", 1, 0)
	env.requirePool("b", 1, 0)
}

// Pools are keyed by instance name prefix, so platform queues for
// different platforms draw from the same pool.
func TestInMemoryBuildQueueTokenPoolsSharedAcrossPlatformQueues(t *testing.T) {
	env := newTokenTestEnv(t, &tokenBuildQueueConfigurationForTesting, time.Unix(1000, 0))
	env.registerPool("oranges", 1)
	requireIdle(t, env.synchronize("worker1", platformForTesting, 0, idleWorkerState()))
	requireIdle(t, env.synchronize("worker2", otherPlatformForTesting, 0, idleWorkerState()))

	stream1 := env.execute(&executeOptions{
		hash:          tokenActionHash(1),
		operationName: tokenOperationName(1),
		tokens:        map[string]uint32{"oranges": 1},
	})
	requireExecuting(t, env.synchronize("worker1", platformForTesting, 0, idleWorkerState()), tokenActionHash(1))
	requireOperationStage(t, stream1, tokenOperationName(1), tokenActionHash(1), remoteexecution.ExecutionStage_EXECUTING)

	env.advance(time.Second)
	stream2 := env.execute(&executeOptions{
		hash:          tokenActionHash(2),
		operationName: tokenOperationName(2),
		tokens:        map[string]uint32{"oranges": 1},
		platform:      otherPlatformForTesting,
	})
	requireIdle(t, env.synchronize("worker2", otherPlatformForTesting, 0, idleWorkerState()))
	env.requirePool("oranges", 1, 1)

	// Completion on one platform unblocks the other. The worker
	// of the first platform has nothing to do. The task stays
	// parked until a worker of its platform asks for work.
	env.advance(time.Second)
	requireIdle(t, env.synchronize("worker1", platformForTesting, 0, completedWorkerState(tokenActionHash(1), successfulExecuteResponse())))
	requireOperationStage(t, stream1, tokenOperationName(1), tokenActionHash(1), remoteexecution.ExecutionStage_COMPLETED)
	env.requirePool("oranges", 0, 1)
	requireExecuting(t, env.synchronize("worker2", otherPlatformForTesting, 0, idleWorkerState()), tokenActionHash(2))
	requireOperationStage(t, stream2, tokenOperationName(2), tokenActionHash(2), remoteexecution.ExecutionStage_EXECUTING)
	env.requirePool("oranges", 1, 0)
}

// Tokens released while the oldest waiting tasks belong to a platform
// queue whose workers are all busy go to a waiting task that a worker
// can run now. Setting them aside for the oldest task leaves them
// unused for as long as its workers stay busy, while idle workers of
// other platform queues hold back the tasks parked for them.
func TestInMemoryBuildQueueTokenPoolsBusyPlatformQueueHoldsNoTokens(t *testing.T) {
	env := newTokenTestEnv(t, &tokenBuildQueueConfigurationForTesting, time.Unix(1000, 0))
	env.registerPool("oranges", 2)
	requireIdle(t, env.synchronize("busy", otherPlatformForTesting, 0, idleWorkerState()))
	requireIdle(t, env.synchronize("idle", platformForTesting, 0, idleWorkerState()))

	// Fill the pool with one task on each platform queue.
	streamBusy := env.execute(&executeOptions{
		hash:          tokenActionHash(1),
		operationName: tokenOperationName(1),
		tokens:        map[string]uint32{"oranges": 1},
		platform:      otherPlatformForTesting,
	})
	requireExecuting(t, env.synchronize("busy", otherPlatformForTesting, 0, idleWorkerState()), tokenActionHash(1))
	requireOperationStage(t, streamBusy, tokenOperationName(1), tokenActionHash(1), remoteexecution.ExecutionStage_EXECUTING)
	env.advance(time.Second)
	streamIdle := env.execute(&executeOptions{
		hash:          tokenActionHash(2),
		operationName: tokenOperationName(2),
		tokens:        map[string]uint32{"oranges": 1},
	})
	requireExecuting(t, env.synchronize("idle", platformForTesting, 0, idleWorkerState()), tokenActionHash(2))
	requireOperationStage(t, streamIdle, tokenOperationName(2), tokenActionHash(2), remoteexecution.ExecutionStage_EXECUTING)

	// Park two tasks for the busy platform queue, followed by two
	// for the other one.
	streams := map[int]remoteexecution.Execution_ExecuteClient{}
	for _, i := range []int{3, 4, 5, 6} {
		p := platformForTesting
		if i < 5 {
			p = otherPlatformForTesting
		}
		env.advance(time.Second)
		streams[i] = env.execute(&executeOptions{
			hash:          tokenActionHash(i),
			operationName: tokenOperationName(i),
			tokens:        map[string]uint32{"oranges": 1},
			platform:      p,
		})
	}
	env.requirePool("oranges", 2, 4)

	// Completing the task on the platform queue with a free worker
	// releases a token. The tasks of the busy platform queue are
	// older, but no worker can run them, so the same worker takes
	// the oldest task of its own platform queue.
	env.advance(time.Second)
	requireExecuting(t, env.synchronize("idle", platformForTesting, 0, completedWorkerState(tokenActionHash(2), successfulExecuteResponse())), tokenActionHash(5))
	requireOperationStage(t, streamIdle, tokenOperationName(2), tokenActionHash(2), remoteexecution.ExecutionStage_COMPLETED)
	requireOperationStage(t, streams[5], tokenOperationName(5), tokenActionHash(5), remoteexecution.ExecutionStage_EXECUTING)
	env.requirePool("oranges", 2, 3)

	// The busy platform queue's worker finishes and takes its
	// oldest waiting task: the order among the tasks that can run
	// is still the order in which they were parked.
	env.advance(time.Second)
	requireExecuting(t, env.synchronize("busy", otherPlatformForTesting, 0, completedWorkerState(tokenActionHash(1), successfulExecuteResponse())), tokenActionHash(3))
	requireOperationStage(t, streamBusy, tokenOperationName(1), tokenActionHash(1), remoteexecution.ExecutionStage_COMPLETED)
	requireOperationStage(t, streams[3], tokenOperationName(3), tokenActionHash(3), remoteexecution.ExecutionStage_EXECUTING)
	env.requirePool("oranges", 2, 2)
}

// When workers of several size classes wait for the same token, the
// task of the smallest size class gets it, even if a task of a larger
// size class was parked first. Otherwise the larger, longer running
// actions end up holding the tokens while small ones queue.
func TestInMemoryBuildQueueTokenPoolsSmallestSizeClassFirst(t *testing.T) {
	// Idle workers block in Synchronize() instead of returning,
	// as the harness only fires timers of the default interval.
	configuration := tokenBuildQueueConfigurationForTesting
	configuration.GetIdleWorkerSynchronizationInterval = func() time.Duration { return time.Minute }
	env := newTokenTestEnv(t, &configuration, time.Unix(1000, 0))
	env.registerPool("oranges", 1)
	env.registerPlatformQueue(platformForTesting, 0, []uint32{1, 2})

	streamX := env.execute(&executeOptions{
		hash:           tokenActionHash(1),
		operationName:  tokenOperationName(1),
		tokens:         map[string]uint32{"oranges": 1},
		sizeClasses:    []uint32{1, 2},
		sizeClassIndex: 1,
	})
	requireExecuting(t, env.synchronize("large1", platformForTesting, 2, idleWorkerState()), tokenActionHash(1))
	requireOperationStage(t, streamX, tokenOperationName(1), tokenActionHash(1), remoteexecution.ExecutionStage_EXECUTING)

	// Park a large task, then a small one.
	env.advance(time.Second)
	streamLarge := env.execute(&executeOptions{
		hash:           tokenActionHash(2),
		operationName:  tokenOperationName(2),
		tokens:         map[string]uint32{"oranges": 1},
		sizeClasses:    []uint32{1, 2},
		sizeClassIndex: 1,
	})
	env.advance(time.Second)
	streamSmall := env.execute(&executeOptions{
		hash:           tokenActionHash(3),
		operationName:  tokenOperationName(3),
		tokens:         map[string]uint32{"oranges": 1},
		sizeClasses:    []uint32{1, 2},
		sizeClassIndex: 0,
	})
	env.requirePool("oranges", 1, 2)

	// A worker of each size class waits for work.
	type synchronizeResult struct {
		response *remoteworker.SynchronizeResponse
		err      error
	}
	waitForWork := func(worker string, sizeClass uint32) (context.CancelFunc, <-chan synchronizeResult) {
		ctx, cancel := context.WithCancel(env.ctx)
		results := make(chan synchronizeResult, 1)
		go func() {
			response, err := env.bq.Synchronize(ctx, &remoteworker.SynchronizeRequest{
				WorkerId:           tokenWorkerID(worker),
				InstanceNamePrefix: "main",
				Platform:           platformForTesting,
				SizeClass:          sizeClass,
				CurrentState:       idleWorkerState(),
			})
			results <- synchronizeResult{response: response, err: err}
		}()
		return cancel, results
	}
	cancelSmall, smallResults := waitForWork("small1", 1)
	defer cancelSmall()
	cancelLarge, largeResults := waitForWork("large2", 2)
	defer cancelLarge()
	require.Eventually(t, func() bool {
		response, err := env.bq.ListPlatformQueues(env.ctx, &emptypb.Empty{})
		require.NoError(t, err)
		for _, scq := range response.PlatformQueues[0].SizeClassQueues {
			if scq.RootInvocation.IdleSynchronizingWorkersCount != 1 {
				return false
			}
		}
		return true
	}, 10*time.Second, time.Millisecond)

	// Releasing the token hands it to the small task.
	env.advance(time.Second)
	requireIdle(t, env.synchronizeWithPreference("large1", platformForTesting, 2, completedWorkerState(tokenActionHash(1), successfulExecuteResponse()), true))
	requireOperationStage(t, streamX, tokenOperationName(1), tokenActionHash(1), remoteexecution.ExecutionStage_COMPLETED)
	var smallResult synchronizeResult
	select {
	case smallResult = <-smallResults:
	case largeResult := <-largeResults:
		require.FailNow(t, "the token went to the large worker", "%v", largeResult.response)
	}
	require.NoError(t, smallResult.err)
	requireExecuting(t, smallResult.response, tokenActionHash(3))
	requireOperationStage(t, streamSmall, tokenOperationName(3), tokenActionHash(3), remoteexecution.ExecutionStage_EXECUTING)
	env.requirePool("oranges", 1, 1)
	require.Equal(t, "oranges", env.operation(tokenOperationName(2)).BlockedOnToken)
	cancelLarge()
	require.Equal(t, codes.Canceled, status.Code((<-largeResults).err))

	// Once no small task is left, the large one gets the token.
	env.advance(time.Second)
	requireIdle(t, env.synchronizeWithPreference("small1", platformForTesting, 1, completedWorkerState(tokenActionHash(3), successfulExecuteResponse()), true))
	requireOperationStage(t, streamSmall, tokenOperationName(3), tokenActionHash(3), remoteexecution.ExecutionStage_COMPLETED)
	requireExecuting(t, env.synchronize("large1", platformForTesting, 2, idleWorkerState()), tokenActionHash(2))
	requireOperationStage(t, streamLarge, tokenOperationName(2), tokenActionHash(2), remoteexecution.ExecutionStage_EXECUTING)
	env.requirePool("oranges", 1, 0)
}

// Killing a parked operation removes it from the pool's FIFO and
// reports the status to the client.
func TestInMemoryBuildQueueTokenPoolsKillParkedOperation(t *testing.T) {
	env := newTokenTestEnv(t, &tokenBuildQueueConfigurationForTesting, time.Unix(1000, 0))
	env.registerPool("oranges", 1)
	requireIdle(t, env.synchronize("worker1", platformForTesting, 0, idleWorkerState()))

	stream1 := env.execute(&executeOptions{
		hash:          tokenActionHash(1),
		operationName: tokenOperationName(1),
		tokens:        map[string]uint32{"oranges": 1},
	})
	requireExecuting(t, env.synchronize("worker1", platformForTesting, 0, idleWorkerState()), tokenActionHash(1))
	requireOperationStage(t, stream1, tokenOperationName(1), tokenActionHash(1), remoteexecution.ExecutionStage_EXECUTING)

	learner2 := mock.NewMockLearner(env.ctrl)
	env.advance(time.Second)
	stream2 := env.execute(&executeOptions{
		hash:          tokenActionHash(2),
		operationName: tokenOperationName(2),
		tokens:        map[string]uint32{"oranges": 1},
		learner:       learner2,
	})
	env.requirePool("oranges", 1, 1)

	learner2.EXPECT().Abandoned()
	_, err := env.bq.KillOperations(env.ctx, &buildqueuestate.KillOperationsRequest{
		Filter: &buildqueuestate.KillOperationsRequest_Filter{
			Type: &buildqueuestate.KillOperationsRequest_Filter_OperationName{
				OperationName: tokenOperationName(2),
			},
		},
		Status: status.New(codes.Unavailable, "Operation was killed administratively").Proto(),
	})
	require.NoError(t, err)
	executeResponse := requireOperationStage(t, stream2, tokenOperationName(2), tokenActionHash(2), remoteexecution.ExecutionStage_COMPLETED)
	testutil.RequireEqualProto(t, status.New(codes.Unavailable, "Operation was killed administratively").Proto(), executeResponse.Status)
	env.requirePool("oranges", 1, 0)
	require.Equal(t, []string{tokenOperationName(1)}, env.listOperations(remoteexecution.ExecutionStage_EXECUTING, "oranges"))
}

// Killing all operations of a size class queue without workers also
// cancels the parked ones.
func TestInMemoryBuildQueueTokenPoolsKillSizeClassQueueWithoutWorkers(t *testing.T) {
	env := newTokenTestEnv(t, &tokenBuildQueueConfigurationForTesting, time.Unix(1000, 0))
	env.registerPool("oranges", 1)
	env.registerPlatformQueue(platformForTesting, 0, []uint32{0})
	env.registerPlatformQueue(otherPlatformForTesting, 0, []uint32{0})
	requireIdle(t, env.synchronize("worker1", otherPlatformForTesting, 0, idleWorkerState()))

	// Exhaust the pool from the other platform queue, so that a
	// task in the workerless queue is parked.
	stream1 := env.execute(&executeOptions{
		hash:          tokenActionHash(1),
		operationName: tokenOperationName(1),
		tokens:        map[string]uint32{"oranges": 1},
		platform:      otherPlatformForTesting,
	})
	requireExecuting(t, env.synchronize("worker1", otherPlatformForTesting, 0, idleWorkerState()), tokenActionHash(1))
	requireOperationStage(t, stream1, tokenOperationName(1), tokenActionHash(1), remoteexecution.ExecutionStage_EXECUTING)

	env.advance(time.Second)
	stream2 := env.execute(&executeOptions{
		hash:          tokenActionHash(2),
		operationName: tokenOperationName(2),
		tokens:        map[string]uint32{"oranges": 1},
	})
	env.advance(time.Second)
	stream3 := env.execute(&executeOptions{
		hash:          tokenActionHash(3),
		operationName: tokenOperationName(3),
	})
	env.requirePool("oranges", 1, 1)

	_, err := env.bq.KillOperations(env.ctx, &buildqueuestate.KillOperationsRequest{
		Filter: &buildqueuestate.KillOperationsRequest_Filter{
			Type: &buildqueuestate.KillOperationsRequest_Filter_SizeClassQueueWithoutWorkers{
				SizeClassQueueWithoutWorkers: &buildqueuestate.SizeClassQueueName{
					PlatformQueueName: &buildqueuestate.PlatformQueueName{
						InstanceNamePrefix: "main",
						Platform:           platformForTesting,
					},
				},
			},
		},
		Status: status.New(codes.Unavailable, "No workers").Proto(),
	})
	require.NoError(t, err)
	for i, stream := range []remoteexecution.Execution_ExecuteClient{stream2, stream3} {
		executeResponse := requireOperationStage(t, stream, tokenOperationName(i+2), tokenActionHash(i+2), remoteexecution.ExecutionStage_COMPLETED)
		testutil.RequireEqualProto(t, status.New(codes.Unavailable, "No workers").Proto(), executeResponse.Status)
	}
	env.requirePool("oranges", 1, 0)
}

// A client that abandons a parked operation causes it to be removed
// after the usual timeout, without disturbing the pool.
func TestInMemoryBuildQueueTokenPoolsAbandonParkedOperation(t *testing.T) {
	env := newTokenTestEnv(t, &tokenBuildQueueConfigurationForTesting, time.Unix(1000, 0))
	env.registerPool("oranges", 1)
	requireIdle(t, env.synchronize("worker1", platformForTesting, 0, idleWorkerState()))

	stream1 := env.execute(&executeOptions{
		hash:          tokenActionHash(1),
		operationName: tokenOperationName(1),
		tokens:        map[string]uint32{"oranges": 1},
	})
	requireExecuting(t, env.synchronize("worker1", platformForTesting, 0, idleWorkerState()), tokenActionHash(1))
	requireOperationStage(t, stream1, tokenOperationName(1), tokenActionHash(1), remoteexecution.ExecutionStage_EXECUTING)

	env.advance(time.Second)
	ctxWithCancel, cancel := context.WithCancel(env.ctx)
	stream2, err := env.executeStream(ctxWithCancel, &executeOptions{
		hash:          tokenActionHash(2),
		operationName: tokenOperationName(2),
		tokens:        map[string]uint32{"oranges": 1},
	})
	require.NoError(t, err)
	requireOperationStage(t, stream2, tokenOperationName(2), tokenActionHash(2), remoteexecution.ExecutionStage_QUEUED)
	env.requirePool("oranges", 1, 1)

	// Cancellation is processed asynchronously. Wait until the
	// scheduler has scheduled the operation for removal.
	cancel()
	require.Eventually(t, func() bool {
		return env.operation(tokenOperationName(2)).Timeout != nil
	}, 10*time.Second, time.Millisecond)

	// Keep the worker alive past the operation's removal, so
	// that only the abandoned operation disappears.
	env.advance(30 * time.Second)
	env.synchronize("worker1", platformForTesting, 0, executingWorkerState(tokenActionHash(1)))
	env.advance(35 * time.Second)
	require.Equal(t, []string{tokenOperationName(1)}, env.listOperations(remoteexecution.ExecutionStage_UNKNOWN, ""))
	env.requirePool("oranges", 1, 0)
}

// A request that is in-flight deduplicated against a parked task is
// attached to it without being enqueued, and both clients observe the
// task starting once tokens become available.
func TestInMemoryBuildQueueTokenPoolsDeduplicateOntoParkedTask(t *testing.T) {
	env := newTokenTestEnv(t, &tokenBuildQueueConfigurationForTesting, time.Unix(1000, 0))
	env.registerPool("oranges", 1)
	requireIdle(t, env.synchronize("worker1", platformForTesting, 0, idleWorkerState()))

	stream1 := env.execute(&executeOptions{
		hash:          tokenActionHash(1),
		operationName: tokenOperationName(1),
		tokens:        map[string]uint32{"oranges": 1},
		invocationID:  "0f0f22ec-908a-4ea7-8a78-b92ab4188e78",
	})
	requireExecuting(t, env.synchronize("worker1", platformForTesting, 0, idleWorkerState()), tokenActionHash(1))
	requireOperationStage(t, stream1, tokenOperationName(1), tokenActionHash(1), remoteexecution.ExecutionStage_EXECUTING)

	env.advance(time.Second)
	stream2 := env.execute(&executeOptions{
		hash:          tokenActionHash(2),
		operationName: tokenOperationName(2),
		tokens:        map[string]uint32{"oranges": 1},
		invocationID:  "0f0f22ec-908a-4ea7-8a78-b92ab4188e78",
	})
	env.requirePool("oranges", 1, 1)

	env.advance(time.Second)
	stream3 := env.execute(&executeOptions{
		hash:              tokenActionHash(2),
		operationName:     tokenOperationName(3),
		tokens:            map[string]uint32{"oranges": 1},
		invocationID:      "e4896008-d596-44c7-8df6-6ced53dff6b0",
		selectorAbandoned: true,
	})
	env.requirePool("oranges", 1, 1)
	require.Equal(t, "oranges", env.operation(tokenOperationName(3)).BlockedOnToken)

	// Both invocations are active, but neither has queued
	// operations in the heaps.
	invocationName := &buildqueuestate.InvocationName{
		SizeClassQueueName: &buildqueuestate.SizeClassQueueName{
			PlatformQueueName: &buildqueuestate.PlatformQueueName{
				InstanceNamePrefix: "main",
				Platform:           platformForTesting,
			},
		},
	}
	children, err := env.bq.ListInvocationChildren(env.ctx, &buildqueuestate.ListInvocationChildrenRequest{
		InvocationName: invocationName,
		Filter:         buildqueuestate.ListInvocationChildrenRequest_ACTIVE,
	})
	require.NoError(t, err)
	require.Len(t, children.Children, 2)
	for _, child := range children.Children {
		require.Equal(t, uint32(1), child.State.BlockedOperationsCount)
		require.Equal(t, uint32(0), child.State.QueuedOperationsCount.Direct)
	}
	children, err = env.bq.ListInvocationChildren(env.ctx, &buildqueuestate.ListInvocationChildrenRequest{
		InvocationName: invocationName,
		Filter:         buildqueuestate.ListInvocationChildrenRequest_QUEUED,
	})
	require.NoError(t, err)
	require.Empty(t, children.Children)

	env.advance(time.Second)
	requireExecuting(t, env.synchronize("worker1", platformForTesting, 0, completedWorkerState(tokenActionHash(1), successfulExecuteResponse())), tokenActionHash(2))
	requireOperationStage(t, stream1, tokenOperationName(1), tokenActionHash(1), remoteexecution.ExecutionStage_COMPLETED)
	requireOperationStage(t, stream2, tokenOperationName(2), tokenActionHash(2), remoteexecution.ExecutionStage_EXECUTING)
	requireOperationStage(t, stream3, tokenOperationName(3), tokenActionHash(2), remoteexecution.ExecutionStage_EXECUTING)
	env.requirePool("oranges", 1, 0)
	children, err = env.bq.ListInvocationChildren(env.ctx, &buildqueuestate.ListInvocationChildrenRequest{
		InvocationName: invocationName,
		Filter:         buildqueuestate.ListInvocationChildrenRequest_ACTIVE,
	})
	require.NoError(t, err)
	require.Len(t, children.Children, 2)
	for _, child := range children.Children {
		require.Equal(t, uint32(0), child.State.BlockedOperationsCount)
		require.Equal(t, uint32(1), child.State.ExecutingWorkersCount)
	}
}

// A task that fails on a small size class releases its tokens and
// acquires them again when it runs on the largest size class.
func TestInMemoryBuildQueueTokenPoolsSizeClassRetry(t *testing.T) {
	env := newTokenTestEnv(t, &tokenBuildQueueConfigurationForTesting, time.Unix(1000, 0))
	env.registerPool("oranges", 1)
	env.registerPlatformQueue(platformForTesting, 0, []uint32{8})
	requireIdle(t, env.synchronize("small", platformForTesting, 3, idleWorkerState()))
	requireIdle(t, env.synchronize("large", platformForTesting, 8, idleWorkerState()))

	learner1 := mock.NewMockLearner(env.ctrl)
	stream := env.execute(&executeOptions{
		hash:          tokenActionHash(1),
		operationName: tokenOperationName(1),
		tokens:        map[string]uint32{"oranges": 1},
		sizeClasses:   []uint32{3, 8},
		learner:       learner1,
	})
	requireExecuting(t, env.synchronize("small", platformForTesting, 3, idleWorkerState()), tokenActionHash(1))
	requireOperationStage(t, stream, tokenOperationName(1), tokenActionHash(1), remoteexecution.ExecutionStage_EXECUTING)
	env.requirePool("oranges", 1, 0)

	// Failure on the small size class moves the task back to the
	// QUEUED stage on the largest size class. No tokens are held
	// in between.
	learner2 := mock.NewMockLearner(env.ctrl)
	learner1.EXPECT().Failed(false).Return(2*time.Minute, 5*time.Minute, learner2)
	env.advance(time.Second)
	requireIdle(t, env.synchronize("small", platformForTesting, 3, completedWorkerState(tokenActionHash(1), &remoteexecution.ExecuteResponse{
		Result: &remoteexecution.ActionResult{ExitCode: 1},
	})))
	requireOperationStage(t, stream, tokenOperationName(1), tokenActionHash(1), remoteexecution.ExecutionStage_QUEUED)
	env.requirePool("oranges", 0, 0)

	requireExecuting(t, env.synchronize("large", platformForTesting, 8, idleWorkerState()), tokenActionHash(1))
	requireOperationStage(t, stream, tokenOperationName(1), tokenActionHash(1), remoteexecution.ExecutionStage_EXECUTING)
	env.requirePool("oranges", 1, 0)

	learner2.EXPECT().Succeeded(gomock.Any(), []uint32{3, 8}).Return(0, time.Duration(0), time.Duration(0), nil)
	env.advance(time.Second)
	requireIdle(t, env.synchronize("large", platformForTesting, 8, completedWorkerState(tokenActionHash(1), successfulExecuteResponse())))
	requireOperationStage(t, stream, tokenOperationName(1), tokenActionHash(1), remoteexecution.ExecutionStage_COMPLETED)
	env.requirePool("oranges", 0, 0)
}

// Background learning re-executes actions nobody waits for. That would
// spend tokens, so it is skipped for token-requiring actions.
func TestInMemoryBuildQueueTokenPoolsBackgroundLearningSkipped(t *testing.T) {
	env := newTokenTestEnv(t, &tokenBuildQueueConfigurationForTesting, time.Unix(1000, 0))
	env.registerPool("oranges", 1)
	env.registerPlatformQueue(platformForTesting, 10, []uint32{8})
	requireIdle(t, env.synchronize("large", platformForTesting, 8, idleWorkerState()))

	learner := mock.NewMockLearner(env.ctrl)
	stream := env.execute(&executeOptions{
		hash:           tokenActionHash(1),
		operationName:  tokenOperationName(1),
		tokens:         map[string]uint32{"oranges": 1},
		sizeClasses:    []uint32{8},
		sizeClassIndex: 0,
		learner:        learner,
	})
	requireExecuting(t, env.synchronize("large", platformForTesting, 8, idleWorkerState()), tokenActionHash(1))
	requireOperationStage(t, stream, tokenOperationName(1), tokenActionHash(1), remoteexecution.ExecutionStage_EXECUTING)

	// The learner asks for a background run on the small size
	// class. It is abandoned, and no operation is created for it:
	// the UUID generator is not called.
	backgroundLearner := mock.NewMockLearner(env.ctrl)
	learner.EXPECT().Succeeded(gomock.Any(), []uint32{8}).Return(0, 3*time.Minute, 7*time.Minute, backgroundLearner)
	backgroundLearner.EXPECT().Abandoned()
	env.advance(time.Second)
	requireIdle(t, env.synchronize("large", platformForTesting, 8, completedWorkerState(tokenActionHash(1), successfulExecuteResponse())))
	requireOperationStage(t, stream, tokenOperationName(1), tokenActionHash(1), remoteexecution.ExecutionStage_COMPLETED)
	env.requirePool("oranges", 0, 0)
	require.Equal(t, []string{tokenOperationName(1)}, env.listOperations(remoteexecution.ExecutionStage_UNKNOWN, ""))
}

// A worker that stops synchronizing has its task completed, which
// returns the task's tokens.
func TestInMemoryBuildQueueTokenPoolsStaleWorkerReleasesTokens(t *testing.T) {
	env := newTokenTestEnv(t, &tokenBuildQueueConfigurationForTesting, time.Unix(1000, 0))
	env.registerPool("oranges", 1)
	requireIdle(t, env.synchronize("worker1", platformForTesting, 0, idleWorkerState()))

	stream := env.execute(&executeOptions{
		hash:          tokenActionHash(1),
		operationName: tokenOperationName(1),
		tokens:        map[string]uint32{"oranges": 1},
	})
	requireExecuting(t, env.synchronize("worker1", platformForTesting, 0, idleWorkerState()), tokenActionHash(1))
	requireOperationStage(t, stream, tokenOperationName(1), tokenActionHash(1), remoteexecution.ExecutionStage_EXECUTING)
	env.requirePool("oranges", 1, 0)

	env.advance(2 * time.Minute)
	env.requirePool("oranges", 0, 0)
	executeResponse := requireOperationStage(t, stream, tokenOperationName(1), tokenActionHash(1), remoteexecution.ExecutionStage_COMPLETED)
	require.Equal(t, codes.Unavailable, status.FromProto(executeResponse.Status).Code())
}

// During the startup grace period token-requiring tasks are parked
// even if their pools have capacity. Token-free tasks are unaffected.
// When the grace period ends, parked tasks may take their tokens.
func TestInMemoryBuildQueueTokenPoolsStartupGrace(t *testing.T) {
	configuration := tokenBuildQueueConfigurationForTesting
	configuration.TokenPoolStartupGracePeriod = 20 * time.Second
	env := newTokenTestEnv(t, &configuration, time.Unix(1000, 0))
	env.registerPool("oranges", 1)
	requireIdle(t, env.synchronize("worker1", platformForTesting, 0, idleWorkerState()))

	env.advance(5 * time.Second)
	stream1 := env.execute(&executeOptions{
		hash:          tokenActionHash(1),
		operationName: tokenOperationName(1),
		tokens:        map[string]uint32{"oranges": 1},
	})
	env.requirePool("oranges", 0, 1)
	require.Equal(t, "oranges", env.operation(tokenOperationName(1)).BlockedOnToken)
	requireIdle(t, env.synchronize("worker1", platformForTesting, 0, idleWorkerState()))

	env.advance(time.Second)
	stream2 := env.execute(&executeOptions{
		hash:          tokenActionHash(2),
		operationName: tokenOperationName(2),
	})
	requireExecuting(t, env.synchronize("worker1", platformForTesting, 0, idleWorkerState()), tokenActionHash(2))
	requireOperationStage(t, stream2, tokenOperationName(2), tokenActionHash(2), remoteexecution.ExecutionStage_EXECUTING)

	// The grace period ends at 1020. Any interaction with the
	// scheduler past that time runs the cleanup queue.
	env.advance(15 * time.Second)
	env.requirePool("oranges", 0, 1)
	requireExecuting(t, env.synchronize("worker2", platformForTesting, 0, idleWorkerState()), tokenActionHash(1))
	requireOperationStage(t, stream1, tokenOperationName(1), tokenActionHash(1), remoteexecution.ExecutionStage_EXECUTING)
	env.requirePool("oranges", 1, 0)
}

// BuildQueueState exposes pools, per-operation requirements and the
// token filter of ListOperations.
func TestInMemoryBuildQueueTokenPoolsBuildQueueState(t *testing.T) {
	env := newTokenTestEnv(t, &tokenBuildQueueConfigurationForTesting, time.Unix(1000, 0))
	env.registerPool("oranges", 2)
	env.registerPool("apples", 1)
	require.NoError(t, env.bq.RegisterTokenPool(util.Must(digest.NewInstanceName("other")), "oranges", 3))
	testutil.RequireEqualStatus(t, status.Error(codes.AlreadyExists, "A token pool named \"oranges\" already exists for instance name prefix \"main\""), env.bq.RegisterTokenPool(util.Must(digest.NewInstanceName("main")), "oranges", 1))
	testutil.RequireEqualStatus(t, status.Error(codes.InvalidArgument, "Token pool name must not be empty"), env.bq.RegisterTokenPool(util.Must(digest.NewInstanceName("main")), "", 1))
	requireIdle(t, env.synchronize("worker1", platformForTesting, 0, idleWorkerState()))

	stream1 := env.execute(&executeOptions{
		hash:          tokenActionHash(1),
		operationName: tokenOperationName(1),
		tokens:        map[string]uint32{"apples": 1, "oranges": 1},
	})
	requireExecuting(t, env.synchronize("worker1", platformForTesting, 0, idleWorkerState()), tokenActionHash(1))
	requireOperationStage(t, stream1, tokenOperationName(1), tokenActionHash(1), remoteexecution.ExecutionStage_EXECUTING)
	env.advance(time.Second)
	env.execute(&executeOptions{
		hash:          tokenActionHash(2),
		operationName: tokenOperationName(2),
		tokens:        map[string]uint32{"apples": 1},
	})
	env.advance(time.Second)
	env.execute(&executeOptions{
		hash:          tokenActionHash(3),
		operationName: tokenOperationName(3),
		tokens:        map[string]uint32{"oranges": 1},
	})
	env.advance(time.Second)
	env.execute(&executeOptions{
		hash:          tokenActionHash(4),
		operationName: tokenOperationName(4),
	})

	response, err := env.bq.ListPlatformQueues(env.ctx, &emptypb.Empty{})
	require.NoError(t, err)
	require.Len(t, response.TokenPools, 3)
	testutil.RequireEqualProto(t, &buildqueuestate.TokenPoolState{InstanceNamePrefix: "main", Name: "apples", Capacity: 1, InUse: 1, BlockedTasksCount: 1}, response.TokenPools[0])
	testutil.RequireEqualProto(t, &buildqueuestate.TokenPoolState{InstanceNamePrefix: "main", Name: "oranges", Capacity: 2, InUse: 1, BlockedTasksCount: 0}, response.TokenPools[1])
	testutil.RequireEqualProto(t, &buildqueuestate.TokenPoolState{InstanceNamePrefix: "other", Name: "oranges", Capacity: 3, InUse: 0, BlockedTasksCount: 0}, response.TokenPools[2])

	o := env.operation(tokenOperationName(1))
	require.Len(t, o.TokenRequirements, 2)
	testutil.RequireEqualProto(t, &buildqueuestate.TokenRequirement{Name: "apples", Amount: 1}, o.TokenRequirements[0])
	testutil.RequireEqualProto(t, &buildqueuestate.TokenRequirement{Name: "oranges", Amount: 1}, o.TokenRequirements[1])
	require.Empty(t, o.BlockedOnToken)
	require.Equal(t, "apples", env.operation(tokenOperationName(2)).BlockedOnToken)
	require.Empty(t, env.operation(tokenOperationName(3)).BlockedOnToken)
	require.Empty(t, env.operation(tokenOperationName(4)).TokenRequirements)

	require.Equal(t, []string{tokenOperationName(1)}, env.listOperations(remoteexecution.ExecutionStage_EXECUTING, "apples"))
	require.Equal(t, []string{tokenOperationName(2)}, env.listOperations(remoteexecution.ExecutionStage_QUEUED, "apples"))
	require.Equal(t, []string{tokenOperationName(1), tokenOperationName(3)}, env.listOperations(remoteexecution.ExecutionStage_UNKNOWN, "oranges"))
	require.Equal(t, []string{tokenOperationName(3)}, env.listOperations(remoteexecution.ExecutionStage_QUEUED, "oranges"))
	require.Len(t, env.listOperations(remoteexecution.ExecutionStage_UNKNOWN, ""), 4)
	require.Empty(t, env.listOperations(remoteexecution.ExecutionStage_UNKNOWN, "pears"))
	// Filters match on the pool key, not just the name. Only
	// parked operations match when asked for blocked ones.
	require.Empty(t, env.listOperationsUnderPrefix(remoteexecution.ExecutionStage_UNKNOWN, "other", "oranges", false))
	require.Equal(t, []string{tokenOperationName(2)}, env.listOperationsUnderPrefix(remoteexecution.ExecutionStage_QUEUED, "main", "apples", true))
	require.Empty(t, env.listOperationsUnderPrefix(remoteexecution.ExecutionStage_QUEUED, "main", "oranges", true))
	_, err = env.bq.ListOperations(env.ctx, &buildqueuestate.ListOperationsRequest{
		FilterTokenName:               "oranges",
		FilterTokenInstanceNamePrefix: "//bad",
	})
	testutil.RequireEqualStatus(t, status.Error(codes.InvalidArgument, "Invalid token instance name prefix \"//bad\": Instance name contains redundant slashes"), err)

	// The worker's view of the action lacks the token properties,
	// while the digest is the client's.
	executing := requireExecuting(t, env.synchronize("worker2", platformForTesting, 0, idleWorkerState()), tokenActionHash(3))
	require.True(t, proto.Equal(platformForTesting, executing.Action.Platform))
}
