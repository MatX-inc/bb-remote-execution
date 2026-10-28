package scheduler

import (
	"cmp"
	"fmt"
	"slices"
	"sort"
	"strconv"
	"strings"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-remote-execution/pkg/proto/buildqueuestate"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/prometheus/client_golang/prometheus"

	status_pb "google.golang.org/genproto/googleapis/rpc/status"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// tokenPropertyPrefix is the prefix of the platform properties through
// which actions declare token requirements: "token:<name>" = "<amount>".
const tokenPropertyPrefix = "token:"

var (
	inMemoryBuildQueueTokenPoolCapacity = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "buildbarn",
			Subsystem: "builder",
			Name:      "in_memory_build_queue_token_pool_capacity",
			Help:      "Configured number of tokens in a token pool.",
		},
		[]string{"instance_name_prefix", "token"},
	)
	inMemoryBuildQueueTokenPoolInUse = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "buildbarn",
			Subsystem: "builder",
			Name:      "in_memory_build_queue_token_pool_in_use",
			Help:      "Number of tokens in a token pool held by tasks in the EXECUTING stage.",
		},
		[]string{"instance_name_prefix", "token"},
	)
	inMemoryBuildQueueTokenPoolBlockedTasks = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "buildbarn",
			Subsystem: "builder",
			Name:      "in_memory_build_queue_token_pool_blocked_tasks",
			Help:      "Number of tasks in the QUEUED stage waiting in a token pool's FIFO for tokens to become available.",
		},
		[]string{"instance_name_prefix", "token"},
	)
	// The token name is client-provided and unbounded for unknown
	// pools, so it is not a label.
	inMemoryBuildQueueTokenPoolRejectionsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "buildbarn",
			Subsystem: "builder",
			Name:      "in_memory_build_queue_token_pool_rejections_total",
			Help:      "Number of Execute() requests rejected because they referenced an unknown token pool, or required more tokens than the pool's capacity.",
		},
		[]string{"instance_name_prefix", "reason"},
	)
)

// tokenPoolKey identifies a token pool. Pools are shared by all
// platform queues under the same instance name prefix.
type tokenPoolKey struct {
	instanceNamePrefix digest.InstanceName
	name               string
}

// tokenPool is a counting semaphore over which tasks are scheduled,
// modelling shared external capacity such as floating license seats.
type tokenPool struct {
	key      tokenPoolKey
	capacity uint32

	// Invariant: inUse equals the sum of the amounts over all tasks
	// that require this pool and have a current worker. It is only
	// adjusted in task.acquireTokens() and task.releaseTokens(),
	// which are called from the single place where a task's worker
	// is set and the single place where it is cleared.
	inUse uint32

	// Tasks in the QUEUED stage that wait for this pool, ordered
	// by task.tokenSequence: the order in which they first became
	// blocked on any pool. dispatchParkedTasks() decides when they
	// take tokens.
	blocked []*task

	capacityGauge     prometheus.Gauge
	inUseGauge        prometheus.Gauge
	blockedTasksGauge prometheus.Gauge
}

// tokenRequirement is a task's demand on a single pool.
type tokenRequirement struct {
	pool   *tokenPool
	amount uint32
}

// parsedTokenRequirement is a token requirement as declared by the
// client, before it has been resolved against a pool.
type parsedTokenRequirement struct {
	name   string
	amount uint32
}

// stripTokenRequirements extracts the "token:<name>" platform
// properties from an action. If any are present, it returns a copy of
// the action whose Platform lacks them, so that neither routing nor
// workers observe them. The relative order of the remaining
// properties is preserved, keeping the platform in the normal form
// that platform.NewKey() requires.
func stripTokenRequirements(action *remoteexecution.Action) (*remoteexecution.Action, []parsedTokenRequirement, error) {
	properties := action.GetPlatform().GetProperties()
	var requirements []parsedTokenRequirement
	var remaining []*remoteexecution.Platform_Property
	for idx, property := range properties {
		name, ok := strings.CutPrefix(property.Name, tokenPropertyPrefix)
		if !ok {
			if requirements != nil {
				remaining = append(remaining, property)
			}
			continue
		}
		if requirements == nil {
			requirements = make([]parsedTokenRequirement, 0, 1)
			remaining = append(make([]*remoteexecution.Platform_Property, 0, len(properties)-1), properties[:idx]...)
		}
		if name == "" {
			return nil, nil, status.Errorf(codes.InvalidArgument, "Platform property %#v does not name a token", property.Name)
		}
		for _, existing := range requirements {
			if existing.name == name {
				return nil, nil, status.Errorf(codes.InvalidArgument, "Token %#v is required more than once", name)
			}
		}
		amount, err := strconv.ParseUint(property.Value, 10, 32)
		if err != nil || amount < 1 {
			return nil, nil, status.Errorf(codes.InvalidArgument, "Token %#v has amount %#v, which is not an integer in [1, 4294967295]", name, property.Value)
		}
		requirements = append(requirements, parsedTokenRequirement{
			name:   name,
			amount: uint32(amount),
		})
	}
	if requirements == nil {
		return action, nil, nil
	}
	strippedAction := proto.Clone(action).(*remoteexecution.Action)
	strippedAction.Platform = &remoteexecution.Platform{
		Properties: remaining,
	}
	return strippedAction, requirements, nil
}

// RegisterTokenPool adds a token pool to InMemoryBuildQueue. Pools are
// keyed by instance name prefix and name; all platform queues under
// the prefix share the pool. Actions may not require a token for which
// no pool exists.
func (bq *InMemoryBuildQueue) RegisterTokenPool(instanceNamePrefix digest.InstanceName, name string, capacity uint32) error {
	if name == "" {
		return status.Error(codes.InvalidArgument, "Token pool name must not be empty")
	}
	key := tokenPoolKey{
		instanceNamePrefix: instanceNamePrefix,
		name:               name,
	}

	bq.enter(bq.clock.Now())
	defer bq.leave()

	if _, ok := bq.tokenPools[key]; ok {
		return status.Errorf(codes.AlreadyExists, "A token pool named %#v already exists for instance name prefix %#v", name, instanceNamePrefix.String())
	}
	labels := []string{instanceNamePrefix.String(), name}
	p := &tokenPool{
		key:               key,
		capacity:          capacity,
		capacityGauge:     inMemoryBuildQueueTokenPoolCapacity.WithLabelValues(labels...),
		inUseGauge:        inMemoryBuildQueueTokenPoolInUse.WithLabelValues(labels...),
		blockedTasksGauge: inMemoryBuildQueueTokenPoolBlockedTasks.WithLabelValues(labels...),
	}
	p.capacityGauge.Set(float64(capacity))
	p.inUseGauge.Set(0)
	p.blockedTasksGauge.Set(0)
	bq.tokenPools[key] = p
	return nil
}

// resolveTokenRequirements looks up the pools named by an action's
// token properties under the instance name prefix of the platform
// queue that will execute it.
func (bq *InMemoryBuildQueue) resolveTokenRequirements(instanceNamePrefix digest.InstanceName, parsed []parsedTokenRequirement) ([]tokenRequirement, error) {
	if len(parsed) == 0 {
		return nil, nil
	}
	requirements := make([]tokenRequirement, 0, len(parsed))
	for _, r := range parsed {
		p, ok := bq.tokenPools[tokenPoolKey{
			instanceNamePrefix: instanceNamePrefix,
			name:               r.name,
		}]
		if !ok {
			inMemoryBuildQueueTokenPoolRejectionsTotal.WithLabelValues(instanceNamePrefix.String(), "UnknownPool").Inc()
			return nil, status.Errorf(codes.FailedPrecondition, "No token pool named %#v exists for instance name prefix %#v", r.name, instanceNamePrefix.String())
		}
		if r.amount > p.capacity {
			inMemoryBuildQueueTokenPoolRejectionsTotal.WithLabelValues(instanceNamePrefix.String(), "ExceedsCapacity").Inc()
			return nil, status.Errorf(codes.FailedPrecondition, "Action requires %d tokens of pool %#v for instance name prefix %#v, which exceeds its capacity of %d", r.amount, r.name, instanceNamePrefix.String(), p.capacity)
		}
		requirements = append(requirements, tokenRequirement{
			pool:   p,
			amount: r.amount,
		})
	}
	return requirements, nil
}

// sortedTokenPools returns all token pools ordered by instance name
// prefix and name, so that iteration is deterministic.
func (bq *InMemoryBuildQueue) sortedTokenPools() []*tokenPool {
	pools := make([]*tokenPool, 0, len(bq.tokenPools))
	for _, p := range bq.tokenPools {
		pools = append(pools, p)
	}
	sort.Slice(pools, func(i, j int) bool {
		pi, pj := pools[i].key.instanceNamePrefix.String(), pools[j].key.instanceNamePrefix.String()
		return pi < pj || (pi == pj && pools[i].key.name < pools[j].key.name)
	})
	return pools
}

func (bq *InMemoryBuildQueue) getTokenPoolStates() []*buildqueuestate.TokenPoolState {
	pools := bq.sortedTokenPools()
	states := make([]*buildqueuestate.TokenPoolState, 0, len(pools))
	for _, p := range pools {
		states = append(states, &buildqueuestate.TokenPoolState{
			InstanceNamePrefix: p.key.instanceNamePrefix.String(),
			Name:               p.key.name,
			Capacity:           p.capacity,
			InUse:              p.inUse,
			BlockedTasksCount:  uint32(len(p.blocked)),
		})
	}
	return states
}

// endTokenPoolStartupGrace is called by the cleanup queue once the
// configured startup grace period has passed. Tasks parked during the
// grace period are now reconsidered.
func (bq *InMemoryBuildQueue) endTokenPoolStartupGrace() {
	bq.tokenPoolStartupGraceActive = false
	bq.dispatchParkedTasks(nil, false)
}

// firstInsufficientPool returns the pool that prevents a task that is
// not parked from being assigned to a worker right now, or nil if the
// task may run. Token-free tasks may always run. During the startup
// grace period no token-requiring task may run. Tasks already parked
// do not hold back one that fits, as parked tasks have no claim on
// tokens (see dispatchParkedTasks()).
func (bq *InMemoryBuildQueue) firstInsufficientPool(t *task) *tokenPool {
	if len(t.tokenRequirements) == 0 {
		return nil
	}
	if bq.tokenPoolStartupGraceActive {
		return t.tokenRequirements[0].pool
	}
	return t.firstShortPool()
}

// fits returns whether the pool has room for the requirement. The sum
// is taken in 64 bits, as capacities may approach the uint32 limit.
func (r tokenRequirement) fits() bool {
	return uint64(r.pool.inUse)+uint64(r.amount) <= uint64(r.pool.capacity)
}

// dispatchParkedTasks hands tokens to parked tasks that a worker can
// run right now. Tasks of smaller size classes go first; within a size
// class, tasks go in the order in which they first became blocked.
// Size classes of different platform queues are compared by number.
// This order decides only among tasks that fit: a larger size class
// task may take tokens that a smaller one needs more of than are free.
// In this order:
//
//   - A task that the pools cannot satisfy right now is passed over
//     without holding any tokens, and does not delay the tasks behind
//     it. A steady stream of requests for few tokens may thus starve a
//     request for many, or for several pools; that is accepted, as
//     tokens set aside for it would leave licenses unused.
//   - A task that the pools can satisfy, but for which no worker is
//     available, is passed over without holding any tokens. Setting
//     tokens aside for it would leave them unused for as long as the
//     workers of its size class queue are busy, while workers of
//     other size class queues sit idle next to tasks parked for them.
//   - A task that the pools can satisfy and for which a worker is
//     available is assigned to an idle synchronizing worker.
//
// When pullerSCQ is set, a worker of that size class queue is looking
// for work, and the first task of that size class queue that may run
// is returned for the caller to assign to the worker. When dryRun is
// set, the first task that may run is returned and nothing changes.
func (bq *InMemoryBuildQueue) dispatchParkedTasks(pullerSCQ *sizeClassQueue, dryRun bool) *task {
	if bq.tokenPoolStartupGraceActive || bq.allTokenPoolsExhausted() {
		return nil
	}
	var parked []*task
	for _, p := range bq.tokenPools {
		parked = append(parked, p.blocked...)
	}
	if len(parked) == 0 {
		return nil
	}
	slices.SortFunc(parked, func(a, b *task) int {
		return cmp.Or(
			cmp.Compare(a.getCurrentSizeClassQueue().sizeClass, b.getCurrentSizeClassQueue().sizeClass),
			cmp.Compare(a.tokenSequence, b.tokenSequence),
		)
	})

	for _, t := range parked {
		if q := t.firstShortPool(); q != nil {
			if !dryRun && q != t.blockedPool {
				// Report the pool the task waits for.
				t.blockedPool.removeBlockedTask(t)
				q.insertBlockedTask(t)
			}
			continue
		}
		scq := t.getCurrentSizeClassQueue()
		hasWorker := scq.rootInvocation.hasIdleSynchronizingWorkers()
		if scq == pullerSCQ || (dryRun && hasWorker) {
			return t
		}
		if hasWorker {
			// Unparking after the assignment keeps the
			// task's invocations active throughout. Unlike
			// at Execute(), clients already wait for the
			// task, so they must be told it started.
			t.assignOrEnqueue(bq, false)
			t.unpark()
			t.reportNonFinalStageChange()
			if bq.allTokenPoolsExhausted() {
				return nil
			}
		}
	}
	return nil
}

// firstShortPool returns the first pool of a task that cannot satisfy
// it, or nil if the task may take its tokens.
func (t *task) firstShortPool() *tokenPool {
	for _, r := range t.tokenRequirements {
		if !r.fits() {
			return r.pool
		}
	}
	return nil
}

func (bq *InMemoryBuildQueue) allTokenPoolsExhausted() bool {
	for _, p := range bq.tokenPools {
		if p.inUse < p.capacity {
			return false
		}
	}
	return true
}

// hasIdleSynchronizingWorkers returns whether a worker is blocked in
// Synchronize() on this invocation or one of its children.
func (i *invocation) hasIdleSynchronizingWorkers() bool {
	return len(i.idleSynchronizingWorkers) > 0 || i.idleSynchronizingWorkersChildren.Len() > 0
}

// cancelBlockedTasks completes all parked tasks belonging to a size
// class queue with the provided status. It returns whether any task
// was cancelled. Completing a task releases its tokens, which may
// cause other parked tasks of the same size class queue to be
// enqueued, so callers cancel queued and parked tasks alternately
// until neither remain.
func (bq *InMemoryBuildQueue) cancelBlockedTasks(scq *sizeClassQueue, status *status_pb.Status) bool {
	var tasks []*task
	for _, p := range bq.sortedTokenPools() {
		for _, t := range p.blocked {
			if t.getCurrentSizeClassQueue() == scq {
				tasks = append(tasks, t)
			}
		}
	}
	for _, t := range tasks {
		t.complete(bq, &remoteexecution.ExecuteResponse{Status: status}, false)
	}
	return len(tasks) > 0
}

// insertBlockedTask places a task in the pool's FIFO at the position
// its token sequence dictates. Tasks blocked for the first time carry
// the newest sequence and land at the tail.
func (p *tokenPool) insertBlockedTask(t *task) {
	if t.blockedPool != nil {
		panic("Task is already blocked on a token pool")
	}
	if t.tokenSequence == 0 {
		panic("Task has no token sequence")
	}
	t.blockedPool = p
	idx := sort.Search(len(p.blocked), func(i int) bool {
		return p.blocked[i].tokenSequence > t.tokenSequence
	})
	p.blocked = slices.Insert(p.blocked, idx, t)
	p.blockedTasksGauge.Set(float64(len(p.blocked)))
}

func (p *tokenPool) removeBlockedTask(t *task) {
	if t.blockedPool != p {
		panic("Task is not blocked on this token pool")
	}
	idx := slices.Index(p.blocked, t)
	if idx < 0 {
		panic("Task is not in the token pool's FIFO")
	}
	p.blocked = slices.Delete(p.blocked, idx, idx+1)
	t.blockedPool = nil
	p.blockedTasksGauge.Set(float64(len(p.blocked)))
}

// acquireTokens is called when a task is assigned to a worker. The
// caller has already established that the pools can satisfy the task,
// except when a queued task is completed administratively through a
// temporary worker; the tokens are released immediately afterwards in
// that case.
func (t *task) acquireTokens() {
	for _, r := range t.tokenRequirements {
		r.pool.inUse += r.amount
		r.pool.inUseGauge.Set(float64(r.pool.inUse))
	}
}

// releaseTokens is called when a task's worker is cleared. It returns
// the tokens and hands them to parked tasks.
func (t *task) releaseTokens(bq *InMemoryBuildQueue) {
	if len(t.tokenRequirements) == 0 {
		return
	}
	for _, r := range t.tokenRequirements {
		if r.pool.inUse < r.amount {
			panic("Token pool in-use count invalid")
		}
		r.pool.inUse -= r.amount
		r.pool.inUseGauge.Set(float64(r.pool.inUse))
	}
	bq.dispatchParkedTasks(nil, false)
}

// park removes a task's operations from the invocation heaps and
// places the task in a pool's FIFO. The task remains in the QUEUED
// stage. Parked operations are counted in their invocations as
// blocked, which keeps those invocations active.
//
// A task keeps the token sequence of the first time it was parked, so
// that a task which was unparked and then found the pool exhausted
// again, for example after being moved to another size class queue,
// does not lose its place to tasks that became blocked after it.
func (t *task) park(bq *InMemoryBuildQueue, p *tokenPool) {
	if t.tokenSequence == 0 {
		bq.lastTokenSequence++
		t.tokenSequence = bq.lastTokenSequence
	}
	for _, o := range t.operations {
		o.invocation.incrementBlockedOperationsCount()
		if o.queueIndex >= 0 {
			o.removeQueuedFromInvocation()
		}
	}
	p.insertBlockedTask(t)
}

// unpark removes a parked task from its pool's FIFO. The caller has
// already assigned or enqueued the task, so that its invocations
// remain active when the blocked operations are no longer counted.
func (t *task) unpark() {
	t.blockedPool.removeBlockedTask(t)
	for _, o := range t.operations {
		o.invocation.decrementBlockedOperationsCount()
	}
}

// matchesTokenFilter implements the token related filters of
// ListOperations(): the task must require the named pool and, if
// requested, be parked on it.
func (t *task) matchesTokenFilter(key tokenPoolKey, blockedOnly bool) bool {
	if blockedOnly {
		return t.blockedPool != nil && t.blockedPool.key == key
	}
	for _, r := range t.tokenRequirements {
		if r.pool.key == key {
			return true
		}
	}
	return false
}

func (t *task) getTokenRequirementsState() []*buildqueuestate.TokenRequirement {
	if len(t.tokenRequirements) == 0 {
		return nil
	}
	requirements := make([]*buildqueuestate.TokenRequirement, 0, len(t.tokenRequirements))
	for _, r := range t.tokenRequirements {
		requirements = append(requirements, &buildqueuestate.TokenRequirement{
			Name:   r.pool.key.name,
			Amount: r.amount,
		})
	}
	return requirements
}

// incrementBlockedOperationsCount records that an operation of this
// invocation was parked on a token pool. Like the executing workers
// count, it is a subtree total that is propagated to all ancestors.
func (i *invocation) incrementBlockedOperationsCount() {
	for ; i != nil; i = i.parent {
		i.maybeActivate()
		i.blockedOperationsCount++
	}
}

// decrementBlockedOperationsCount is the inverse of
// incrementBlockedOperationsCount(). It does not remove invocations
// that become empty; callers do so where appropriate.
func (i *invocation) decrementBlockedOperationsCount() {
	for ; i != nil; i = i.parent {
		if i.blockedOperationsCount == 0 {
			panic("Blocked operations count invalid")
		}
		i.blockedOperationsCount--
		i.maybeDeactivate()
	}
}

// enableTokenInvariantChecks causes the token pool invariants to be
// re-derived from first principles every time the build queue lock is
// released. It is set by an internal test, so that every scheduler
// test exercises the checks without paying for them in production.
var enableTokenInvariantChecks bool

func (bq *InMemoryBuildQueue) checkTokenInvariants() {
	expectedInUse := map[*tokenPool]uint32{}
	for _, scq := range bq.sizeClassQueues {
		for _, w := range scq.workers {
			t := w.currentTask
			if t == nil {
				continue
			}
			if t.currentWorker != w {
				panic("Worker and task disagree about their association")
			}
			for _, r := range t.tokenRequirements {
				expectedInUse[r.pool] += r.amount
			}
		}
	}

	expectedBlocked := map[*invocation]uint32{}
	for _, p := range bq.tokenPools {
		if p.inUse != expectedInUse[p] {
			panic(fmt.Sprintf("Token pool %#v has %d tokens in use, but executing tasks hold %d", p.key.name, p.inUse, expectedInUse[p]))
		}
		for idx, t := range p.blocked {
			if t.blockedPool != p {
				panic("Parked task does not point at its token pool")
			}
			if t.tokenSequence == 0 || (idx > 0 && p.blocked[idx-1].tokenSequence >= t.tokenSequence) {
				panic("Token pool FIFO is not ordered by token sequence")
			}
			if t.currentWorker != nil || t.executeResponse != nil {
				panic("Parked task is not in the QUEUED stage")
			}
			for i, o := range t.operations {
				if o.queueIndex >= 0 {
					panic("Parked operation is still in an invocation heap")
				}
				for ; i != nil; i = i.parent {
					expectedBlocked[i]++
				}
			}
		}
	}

	if bq.dispatchParkedTasks(nil, true) != nil {
		panic("A parked task can run on an idle synchronizing worker, but was not assigned to it")
	}

	for _, scq := range bq.sizeClassQueues {
		scq.rootInvocation.checkBlockedOperationsCount(expectedBlocked)
	}
}

func (i *invocation) checkBlockedOperationsCount(expected map[*invocation]uint32) {
	if i.blockedOperationsCount != expected[i] {
		panic(fmt.Sprintf("Invocation has blocked operations count %d, but %d parked operations", i.blockedOperationsCount, expected[i]))
	}
	for _, iChild := range i.children {
		iChild.checkBlockedOperationsCount(expected)
	}
}
