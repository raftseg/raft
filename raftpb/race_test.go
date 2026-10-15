// Copyright 2026 The raftseg authors. Licensed under the Apache License,
// Version 2.0.

//go:build race

package raftpb

// raceEnabled reports whether the race detector is on: it changes how much
// decoding allocates.
const raceEnabled = true
