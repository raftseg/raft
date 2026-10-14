// Copyright 2026 The raftseg authors. Licensed under the Apache License,
// Version 2.0.

package raftpb

import "strconv"

// This fork supports 64-bit platforms only, and this fails the build on any
// other. The generated decoder (unchanged from upstream) accumulates field
// lengths in an int: with 32-bit ints, a crafted length such as 2^33 wraps
// to 0 and a malformed message is accepted where a 64-bit build rejects it.
// Nodes of one cluster must agree on which messages are valid.
const _ uint = strconv.IntSize - 64
