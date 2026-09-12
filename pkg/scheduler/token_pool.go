package scheduler

import (
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
	inMemoryBuildQueueTokenPoolReserved = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "buildbarn",
			Subsystem: "builder",
			Name:      "in_memory_build_queue_token_pool_reserved",
			Help:      "Number of tokens in a token pool reserved for tasks that were taken off the pool's FIFO, but are still waiting for a worker.",
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

	// Tokens promised to tasks that unblock() took off the FIFO
	// and enqueued because no idle worker was available. Without
	// the reservation, a task on another platform queue sharing
	// the pool could take the tokens first and the unblocked
	// task would be parked again, defeating the FIFO. Invariant:
	// reserved equals the sum of the amounts over tasks with
	// tokensReserved set. Converted into inUse on assignment.
	reserved uint32

	// Tasks in the QUEUED stage that could not be assigned to a
	// worker because this pool could not satisfy them, ordered by
	// task.tokenSequence: the order in which they first became
	// blocked on any pool. A task may only take tokens if no task
	// that became blocked before it is still waiting; this
	// head-of-line blocking is what prevents a stream of small
	// requests from starving a large one.
	blocked []*task

	capacityGauge     prometheus.Gauge
	inUseGauge        prometheus.Gauge
	reservedGauge     prometheus.Gauge
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
		reservedGauge:     inMemoryBuildQueueTokenPoolReserved.WithLabelValues(labels...),
		blockedTasksGauge: inMemoryBuildQueueTokenPoolBlockedTasks.WithLabelValues(labels...),
	}
	p.capacityGauge.Set(float64(capacity))
	p.inUseGauge.Set(0)
	p.reservedGauge.Set(0)
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
			Reserved:           p.reserved,
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
	for _, p := range bq.sortedTokenPools() {
		p.unblock(bq)
	}
}

// firstInsufficientPool returns the pool that prevents a task from
// being assigned to a worker right now, or nil if the task may run.
// Token-free tasks may always run. During the startup grace period
// no token-requiring task may run.
func (bq *InMemoryBuildQueue) firstInsufficientPool(t *task) *tokenPool {
	if len(t.tokenRequirements) == 0 {
		return nil
	}
	if bq.tokenPoolStartupGraceActive {
		return t.tokenRequirements[0].pool
	}
	if t.tokensReserved {
		// The pools already set tokens aside for this task
		// when it was taken off a FIFO.
		return nil
	}
	for _, r := range t.tokenRequirements {
		p := r.pool
		// Tasks are served in the order in which they first
		// became blocked. A task that never was blocked ranks
		// after every waiting task.
		if len(p.blocked) > 0 && p.blocked[0] != t && (t.tokenSequence == 0 || p.blocked[0].tokenSequence < t.tokenSequence) {
			return p
		}
		if p.inUse+p.reserved+r.amount > p.capacity {
			return p
		}
	}
	return nil
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

// unblock reconsiders the head of the pool's FIFO after tokens were
// released. Heads that fit everywhere are scheduled, heads that are
// blocked on another pool move to that pool's FIFO, and a head that
// is still blocked here ends the scan.
func (p *tokenPool) unblock(bq *InMemoryBuildQueue) {
	for len(p.blocked) > 0 {
		t := p.blocked[0]
		q := bq.firstInsufficientPool(t)
		if q == p {
			return
		}
		if q != nil {
			// Blocked on another pool: wait there, keeping
			// the token sequence. A task requiring several
			// pools may bounce between their FIFOs on
			// successive releases when the pools fill up
			// alternately. Each bounce is triggered by a
			// release and settles as soon as all pools have
			// room, which is accepted for now.
			p.removeBlockedTask(t)
			q.insertBlockedTask(t)
			continue
		}
		// The task is either handed to an idle worker, which
		// acquires its tokens, or enqueued with the tokens
		// reserved, so that no other task takes them before a
		// worker reaches it. The task is unparked afterwards,
		// so that its invocation stays active throughout and
		// is neither deactivated nor removed.
		t.reserveTokens()
		t.assignOrEnqueue(bq, false)
		t.unpark()
	}
}

// reserveTokens sets tokens aside for a task that passed the gate but
// has no worker yet.
func (t *task) reserveTokens() {
	if t.tokensReserved {
		panic("Task already has tokens reserved")
	}
	t.tokensReserved = true
	for _, r := range t.tokenRequirements {
		r.pool.reserved += r.amount
		r.pool.reservedGauge.Set(float64(r.pool.reserved))
	}
}

// dropReservation is the inverse of reserveTokens(). It is a no-op for
// tasks without a reservation.
func (t *task) dropReservation() {
	if !t.tokensReserved {
		return
	}
	t.tokensReserved = false
	for _, r := range t.tokenRequirements {
		if r.pool.reserved < r.amount {
			panic("Token pool reserved count invalid")
		}
		r.pool.reserved -= r.amount
		r.pool.reservedGauge.Set(float64(r.pool.reserved))
	}
}

// acquireTokens is called when a task is assigned to a worker. Any
// reservation is converted into tokens in use. The caller has already
// established that the pools can satisfy the task, except when a
// queued task is completed administratively through a temporary
// worker; the tokens are released immediately afterwards in that case.
func (t *task) acquireTokens() {
	t.dropReservation()
	for _, r := range t.tokenRequirements {
		r.pool.inUse += r.amount
		r.pool.inUseGauge.Set(float64(r.pool.inUse))
	}
}

// releaseTokens is called when a task's worker is cleared. It returns
// the tokens and lets each pool reconsider its parked tasks.
func (t *task) releaseTokens(bq *InMemoryBuildQueue) {
	for _, r := range t.tokenRequirements {
		if r.pool.inUse < r.amount {
			panic("Token pool in-use count invalid")
		}
		r.pool.inUse -= r.amount
		r.pool.inUseGauge.Set(float64(r.pool.inUse))
	}
	for _, r := range t.tokenRequirements {
		r.pool.unblock(bq)
	}
}

// park removes a task's operations from the invocation heaps and
// places the task in a pool's FIFO. The task remains in the QUEUED
// stage. Parked operations are counted in their invocations as
// blocked, which keeps those invocations active.
//
// A task keeps the token sequence of the first time it was parked, so
// that a task which was unblocked, enqueued and then found the pool
// exhausted again by the time a worker picked it up does not lose its
// place to tasks that became blocked after it.
func (t *task) park(bq *InMemoryBuildQueue, p *tokenPool) {
	if t.tokenSequence == 0 {
		bq.lastTokenSequence++
		t.tokenSequence = bq.lastTokenSequence
	}
	t.dropReservation()
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

	expectedReserved := map[*tokenPool]uint32{}
	seenTasks := map[*task]struct{}{}
	for _, o := range bq.operationsNameMap {
		t := o.task
		if _, ok := seenTasks[t]; ok || !t.tokensReserved {
			continue
		}
		seenTasks[t] = struct{}{}
		if t.currentWorker != nil || t.blockedPool != nil || t.executeResponse != nil {
			panic("Task with reserved tokens is not waiting in a queue")
		}
		for _, r := range t.tokenRequirements {
			expectedReserved[r.pool] += r.amount
		}
	}

	expectedBlocked := map[*invocation]uint32{}
	for _, p := range bq.tokenPools {
		if p.inUse != expectedInUse[p] {
			panic(fmt.Sprintf("Token pool %#v has %d tokens in use, but executing tasks hold %d", p.key.name, p.inUse, expectedInUse[p]))
		}
		if p.reserved != expectedReserved[p] {
			panic(fmt.Sprintf("Token pool %#v has %d tokens reserved, but queued tasks reserve %d", p.key.name, p.reserved, expectedReserved[p]))
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
			if bq.firstInsufficientPool(t) == nil && p.blocked[0] == t {
				panic("Head of token pool FIFO is eligible to run, but was not unblocked")
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
