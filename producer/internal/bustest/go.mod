module github.com/arloliu/tracekeep/producer/internal/bustest

go 1.26.0

require (
	github.com/arloliu/go-secs/tracepack v0.9.0
	github.com/arloliu/tracekeep v0.0.0
	github.com/arloliu/tracekeep/producer v0.1.0
	github.com/nats-io/nats.go v1.55.0
)

require (
	github.com/antithesishq/antithesis-sdk-go v0.8.0-default-no-op // indirect
	github.com/google/go-tpm v0.9.8 // indirect
	github.com/klauspost/compress v1.20.1 // indirect
	github.com/minio/highwayhash v1.0.4 // indirect
	github.com/nats-io/jwt/v2 v2.8.2 // indirect
	github.com/nats-io/nats-server/v2 v2.15.1 // indirect
	github.com/nats-io/nkeys v0.4.16 // indirect
	github.com/nats-io/nuid v1.0.1 // indirect
	golang.org/x/crypto v0.57.0 // indirect
	golang.org/x/sys v0.49.0 // indirect
	golang.org/x/time v0.16.0 // indirect
)

// This module only tests and nothing requires it, so its replace directives reach no other build (T-35).
replace (
	github.com/arloliu/tracekeep => ../../..
	github.com/arloliu/tracekeep/producer => ../..
)
