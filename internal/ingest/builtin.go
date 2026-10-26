package ingest

import (
	"errors"
	"fmt"

	"github.com/arloliu/go-secs/tracepack/classify"
)

// unversioned is what classify.Name reports when the build names no go-secs version, as a workspace build does.
const unversioned = "go-secs/unknown"

// ErrUnversionedBuild refuses a build whose classifier cannot name its version:
// two such builds would share one processing configuration whatever their classifiers (rollouts.md §6.4).
var ErrUnversionedBuild = errors.New("ingest: the build names no classifier version")

// Builtin is the processing configuration this binary executes: the go-secs classifier, named for its implementation and version
// (classify.Name), under maxFrameLen, with no masking policy in phase 1.
// An unversioned build is refused unless allowUnversioned, which only dev passes.
func Builtin(maxFrameLen uint64, allowUnversioned bool) (Config, error) {
	return namedConfig(classify.Name, maxFrameLen, allowUnversioned)
}

func namedConfig(name string, maxFrameLen uint64, allowUnversioned bool) (Config, error) {
	if name == unversioned && !allowUnversioned {
		return Config{}, fmt.Errorf("%w: %s", ErrUnversionedBuild, name)
	}

	return Config{Classifier: classify.NewNamed(name, maxFrameLen), Name: name, MaxFrameLen: maxFrameLen}, nil
}
