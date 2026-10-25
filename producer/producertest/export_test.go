package producertest

import "testing"

// RunChaosReporting is RunChaos reporting each scenario's failures to the TB report returns for it.
func RunChaosReporting(t *testing.T, newDevice NewDevice, report func(t *testing.T) testing.TB) {
	runScenarios(t, newDevice, report)
}
