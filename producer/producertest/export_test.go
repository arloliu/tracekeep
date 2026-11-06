package producertest

import (
	"testing"
	"time"
)

// RunChaosReporting is RunChaos with settle bounding each wait for the bus, reporting each scenario's failures to the TB report returns for it.
func RunChaosReporting(t *testing.T, newDevice NewDevice, settle time.Duration, report func(t *testing.T) testing.TB) {
	runScenarios(t, newDevice, settle, report)
}
