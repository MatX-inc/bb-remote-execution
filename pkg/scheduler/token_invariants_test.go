package scheduler

// Re-derive the token pool invariants every time the build queue lock
// is released, so that every test in this package exercises them.
func init() {
	enableTokenInvariantChecks = true
}
