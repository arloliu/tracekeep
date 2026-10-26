package ingest

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/arloliu/go-secs/tracepack/classify"

	"github.com/arloliu/tracekeep/internal/catalog"
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

// ErrForeignConfiguration refuses a replica that cannot execute a configuration it must, one of another classifier:
// unretired, which the activation gate requires, or bound by a capture the catalog holds (rollouts.md §6.4).
var ErrForeignConfiguration = errors.New("ingest: a required configuration of another classifier")

// Executable is every configuration this replica executes: builtin, and each configuration the catalog requires (RequiredConfigs)
// of builtin's classifier, whatever its frame limit, since the same classifier runs under any limit:
// a limit changed and changed back still registers, and a capture bound to a retired configuration is still executed.
// A required configuration of another classifier is refused before anything is activated, naming it.
func Executable(ctx context.Context, cat *catalog.Catalog, builtin Config) ([]Config, error) {
	stored, err := cat.RequiredConfigs(ctx)
	if err != nil {
		return nil, err
	}
	cs := []Config{builtin}
	for _, s := range stored {
		var content ConfigContent
		if err := json.Unmarshal(s.Content, &content); err != nil {
			return nil, fmt.Errorf("ingest: configuration %d: %w", s.ID, err)
		}
		if content.Classifier != builtin.Name {
			return nil, fmt.Errorf("%w: configuration %d of %s, this replica's classifier being %s", ErrForeignConfiguration, s.ID, content.Classifier, builtin.Name)
		}
		if content.MaxFrameLen != builtin.MaxFrameLen {
			c, err := namedConfig(builtin.Name, content.MaxFrameLen, true)
			if err != nil {
				return nil, err
			}
			cs = append(cs, c)
		}
	}

	return cs, nil
}
