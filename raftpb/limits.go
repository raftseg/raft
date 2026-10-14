// Copyright 2026 The raftseg authors. Licensed under the Apache License,
// Version 2.0.

package raftpb

import "errors"

// MaxMessageNesting is how deeply Message.Responses may nest in an encoded
// Message. raft itself nests one level: a MsgStorageAppend or
// MsgStorageApply carries the messages to send when the work is done, and
// those carry none.
//
// Without a bound, decoding is recursive in the nesting depth, and Go cannot
// recover from running out of stack: about 40 MB of crafted input (8 million
// levels) is enough to kill any process that decodes it.
const MaxMessageNesting = 16

// ErrNestingTooDeep is returned by Message.Unmarshal for input nested more
// than MaxMessageNesting levels deep.
var ErrNestingTooDeep = errors.New("raftpb: Message responses nested too deeply")
