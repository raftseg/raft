// Copyright 2023 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// Modified by the raftseg authors: import paths rewritten for this
// fork's module and testify replaced by internal/testutil.

package rafttest

import (
	"testing"

	"github.com/raftseg/raft/internal/datadriven"
	"github.com/raftseg/raft/internal/testutil/require"

	"github.com/raftseg/raft"
	"github.com/raftseg/raft/raftpb"
)

func (env *InteractionEnv) handleSendSnapshot(t *testing.T, d datadriven.TestData) error {
	idxs := nodeIdxs(t, d)
	require.Len(t, idxs, 2)
	return env.SendSnapshot(idxs[0], idxs[1])
}

// SendSnapshot sends a snapshot.
func (env *InteractionEnv) SendSnapshot(fromIdx, toIdx int) error {
	snap, err := env.Nodes[fromIdx].Snapshot()
	if err != nil {
		return err
	}
	from, to := uint64(fromIdx+1), uint64(toIdx+1)
	fromStatus := env.Nodes[fromIdx].BasicStatus()
	msg := &raftpb.Message{
		Type:     raftpb.MsgSnap.Enum(),
		Term:     new(fromStatus.GetTerm()),
		From:     new(from),
		To:       new(to),
		Snapshot: snap,
	}
	env.Messages = append(env.Messages, msg)
	_, _ = env.Output.WriteString(raft.DescribeMessage(msg, nil))
	return nil
}
