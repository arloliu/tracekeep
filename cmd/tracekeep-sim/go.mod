module github.com/arloliu/tracekeep/cmd/tracekeep-sim

go 1.26.0

// Until the SDK's first tag the simulator is a module of its own that nothing requires,
// so its replace directives reach no other build (T-38).
replace (
	github.com/arloliu/tracekeep => ../..
	github.com/arloliu/tracekeep/producer => ../../producer
)
