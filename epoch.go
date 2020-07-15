/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package mirbft

import (
	"fmt"

	pb "github.com/IBM/mirbft/mirbftpb"
	"github.com/pkg/errors"
)

// intersectionQuorum is the number of nodes required to agree
// such that any two sets intersected will each contain some same
// correct node.  This is ceil((n+f+1)/2), which is equivalent to
// (n+f+2)/2 under truncating integer math.
func intersectionQuorum(nc *pb.NetworkConfig) int {
	return (len(nc.Nodes) + int(nc.F) + 2) / 2
}

// someCorrectQuorum is the number of nodes such that at least one of them is correct
func someCorrectQuorum(nc *pb.NetworkConfig) int {
	return int(nc.F) + 1
}

// logWidth is the number of sequence numbers in the sliding window
func logWidth(nc *pb.NetworkConfig) int {
	return 3 * int(nc.CheckpointInterval)
}

func initialSequence(epochConfig *pb.EpochConfig, networkConfig *pb.NetworkConfig) uint64 {
	if epochConfig.PlannedExpiration > networkConfig.MaxEpochLength {
		return epochConfig.PlannedExpiration - networkConfig.MaxEpochLength + 1
	}
	return 1
}

func seqToBucket(seqNo uint64, ec *pb.EpochConfig, nc *pb.NetworkConfig) BucketID {
	return BucketID((seqNo - initialSequence(ec, nc)) % uint64(nc.NumberOfBuckets))
}

func (e *epoch) seqToBucket(seqNo uint64) BucketID {
	return seqToBucket(seqNo, e.epochConfig, e.networkConfig)
}

func seqToColumn(seqNo uint64, ec *pb.EpochConfig, nc *pb.NetworkConfig) uint64 {
	return (seqNo-initialSequence(ec, nc))/uint64(nc.NumberOfBuckets) + 1
}

func (e *epoch) seqToColumn(seqNo uint64) uint64 {
	return seqToColumn(seqNo, e.epochConfig, e.networkConfig)
}

/*
func (ec *epochConfig) seqToBucketColumn(seqNo uint64) (BucketID, uint64) {
	return ec.seqToBucket(seqNo), ec.seqToColumn(seqNo)
}

func (ec *epochConfig) colBucketToSeq(column uint64, bucket BucketID) uint64 {
	return ec.initialSequence + (column-1)*uint64(len(ec.buckets)) + uint64(bucket)
}
*/

type epoch struct {
	// config contains the static components of the epoch
	epochConfig   *pb.EpochConfig
	networkConfig *pb.NetworkConfig
	myConfig      *Config

	proposer      *proposer
	persisted     *persisted
	clientWindows *clientWindows

	buckets   map[BucketID]NodeID
	sequences []*sequence

	ending            bool // set when this epoch about to end gracefully
	lowestUncommitted int
	lowestUnallocated []int // index by bucket

	lastCommittedAtTick uint64
	ticksSinceProgress  int

	checkpoints       []*checkpoint
	checkpointTracker *checkpointTracker
}

// newEpoch creates a new epoch.  It uses the supplied initial checkpoints until
// new checkpoint windows are created using the given epochConfig.  The initialCheckpoint
// windows may be empty, of length 1, or length 2.
func newEpoch(persisted *persisted, newEpochConfig *pb.EpochConfig, checkpointTracker *checkpointTracker, clientWindows *clientWindows, myConfig *Config) *epoch {
	var startingEntry *logEntry
	var maxCheckpoint *pb.CEntry
	var epochConfig *pb.EpochConfig

	for head := persisted.logHead; head != nil; head = head.next {
		switch d := head.entry.Type.(type) {
		case *pb.Persisted_NewEpochStart:
			epochConfig = d.NewEpochStart
		case *pb.Persisted_CEntry:
			startingEntry = head.next
			maxCheckpoint = d.CEntry
			epochConfig = d.CEntry.EpochConfig
		}
	}

	networkConfig := maxCheckpoint.NetworkConfig

	buckets := map[BucketID]NodeID{}

	leaders := map[uint64]struct{}{}
	for _, leader := range newEpochConfig.Leaders {
		leaders[leader] = struct{}{}
	}

	overflowIndex := 0 // TODO, this should probably start after the last assigned node
	for i := 0; i < int(networkConfig.NumberOfBuckets); i++ {
		bucketID := BucketID(i)
		leader := networkConfig.Nodes[(uint64(i)+newEpochConfig.Number)%uint64(len(networkConfig.Nodes))]
		if _, ok := leaders[leader]; !ok {
			buckets[bucketID] = NodeID(newEpochConfig.Leaders[overflowIndex%len(newEpochConfig.Leaders)])
			overflowIndex++
		} else {
			buckets[bucketID] = NodeID(leader)
		}
	}

	lowestUnallocated := make([]int, len(buckets))
	for i := range lowestUnallocated {
		lowestUnallocated[int(seqToBucket(maxCheckpoint.SeqNo+uint64(i+1), epochConfig, networkConfig))] = i
	}

	checkpoints := []*checkpoint{checkpointTracker.checkpoint(maxCheckpoint.SeqNo)}

	sequences := make([]*sequence, logWidth(networkConfig))
	for i := range sequences {
		seqNo := maxCheckpoint.SeqNo + uint64(i+1)
		bucket := seqToBucket(seqNo, epochConfig, networkConfig)
		owner := buckets[bucket]
		sequences[i] = newSequence(owner, epochConfig.Number, seqNo, clientWindows, persisted, networkConfig, myConfig)
		if seqNo%uint64(networkConfig.CheckpointInterval) == 0 {
			checkpoints = append(checkpoints, checkpointTracker.checkpoint(seqNo))
		}
	}

	for logEntry := startingEntry; logEntry != nil; logEntry = logEntry.next {
		switch d := logEntry.entry.Type.(type) {
		case *pb.Persisted_QEntry:
			offset := int(d.QEntry.SeqNo-maxCheckpoint.SeqNo) - 1
			if offset < 0 || offset >= len(sequences) {
				panic("should never be possible") // TODO, improve
			}
			bucket := seqToBucket(d.QEntry.SeqNo, epochConfig, networkConfig)
			lowestUnallocated[bucket] = offset + len(buckets)
			sequences[offset].qEntry = d.QEntry
			sequences[offset].digest = d.QEntry.Digest
			sequences[offset].state = Preprepared
		case *pb.Persisted_PEntry:
			offset := int(d.PEntry.SeqNo-maxCheckpoint.SeqNo) - 1
			if offset < 0 || offset >= len(sequences) {
				panic("should never be possible") // TODO, improve
			}

			if persisted.lastCommitted >= d.PEntry.SeqNo {
				sequences[offset].state = Committed
			} else {
				sequences[offset].state = Prepared
			}
		}
	}

	lowestUncommitted := int(persisted.lastCommitted - maxCheckpoint.SeqNo)

	proposer := newProposer(myConfig, clientWindows, buckets)
	proposer.stepAllClientWindows()

	return &epoch{
		buckets:           buckets,
		myConfig:          myConfig,
		epochConfig:       epochConfig,
		networkConfig:     networkConfig,
		checkpointTracker: checkpointTracker,
		checkpoints:       checkpoints,
		clientWindows:     clientWindows,
		persisted:         persisted,
		proposer:          proposer,
		sequences:         sequences,
		lowestUnallocated: lowestUnallocated,
		lowestUncommitted: lowestUncommitted,
	}
}

func (e *epoch) getSequence(seqNo uint64) (*sequence, int, error) {
	if seqNo < e.lowWatermark() || seqNo > e.highWatermark() {
		return nil, 0, errors.Errorf("requested seq no (%d) is out of range [%d - %d]",
			seqNo, e.lowWatermark(), e.highWatermark())
	}
	offset := int(seqNo - e.lowWatermark())
	return e.sequences[offset], offset, nil
}

func (e *epoch) applyPreprepareMsg(source NodeID, seqNo uint64, batch []*pb.RequestAck) *Actions {
	seq, offset, err := e.getSequence(seqNo)
	if err != nil {
		e.myConfig.Logger.Error(err.Error())
		return &Actions{}
	}

	bucketID := e.seqToBucket(seqNo)

	if source == NodeID(e.myConfig.ID) {
		// Apply our own preprepares as a prepare
		return seq.applyPrepareMsg(source, seq.digest)
	}

	defer func() {
		e.lowestUnallocated[int(bucketID)] += len(e.buckets)
	}()

	if offset != e.lowestUnallocated[int(bucketID)] {
		panic(fmt.Sprintf("dev test, this really shouldn't happen: offset=%d e.lowestUnallocated=%d\n", offset, e.lowestUnallocated[int(bucketID)]))
	}

	return seq.allocate(batch)
}

func (e *epoch) applyPrepareMsg(source NodeID, seqNo uint64, digest []byte) *Actions {
	seq, _, err := e.getSequence(seqNo)
	if err != nil {
		e.myConfig.Logger.Error(err.Error())
		return &Actions{}
	}
	return seq.applyPrepareMsg(source, digest)
}

func (e *epoch) applyCommitMsg(source NodeID, seqNo uint64, digest []byte) *Actions {
	seq, offset, err := e.getSequence(seqNo)
	if err != nil {
		e.myConfig.Logger.Error(err.Error())
		return &Actions{}
	}

	seq.applyCommitMsg(source, digest)
	if seq.state != Committed || offset != e.lowestUncommitted {
		return &Actions{}
	}

	actions := &Actions{}

	for e.lowestUncommitted < len(e.sequences) {
		if e.sequences[e.lowestUncommitted].state != Committed {
			break
		}

		actions.Commits = append(actions.Commits, &Commit{
			QEntry:        e.sequences[e.lowestUncommitted].qEntry,
			Checkpoint:    e.sequences[e.lowestUncommitted].seqNo%uint64(e.networkConfig.CheckpointInterval) == 0,
			NetworkConfig: e.networkConfig,
			EpochConfig:   e.epochConfig,
		})
		for _, reqForward := range e.sequences[e.lowestUncommitted].qEntry.Requests {
			cw, ok := e.clientWindows.clientWindow(reqForward.Request.ClientId)
			if !ok {
				panic("we never should have committed this without the client available")
			}
			cw.request(reqForward.Request.ReqNo).committed = &seqNo
		}

		e.persisted.setLastCommitted(e.sequences[e.lowestUncommitted].seqNo)
		e.lowestUncommitted++
	}

	return actions
}

func (e *epoch) moveWatermarks() *Actions {
	ci := int(e.networkConfig.CheckpointInterval)

	for len(e.checkpoints) >= 4 && e.checkpoints[1].stable {
		e.checkpoints = e.checkpoints[1:]
		e.sequences = e.sequences[ci:]
		e.lowestUncommitted -= ci
		for i := range e.lowestUnallocated {
			e.lowestUnallocated[i] -= ci
		}
	}

	lastCW := e.checkpoints[len(e.checkpoints)-1]
	oldLastCW := lastCW

	// If this epoch is ending, don't allocate new sequences
	if lastCW.seqNo == e.epochConfig.PlannedExpiration {
		e.ending = true
		return &Actions{}
	}

	for len(e.checkpoints) < 4 {
		e.checkpoints = append(e.checkpoints, e.checkpointTracker.checkpoint(lastCW.seqNo+uint64(ci)))
		lastCW = e.checkpoints[len(e.checkpoints)-1]
	}

	for i := oldLastCW.seqNo + 1; i <= lastCW.seqNo; i++ {
		seqNo := i
		epoch := e.epochConfig.Number
		owner := e.buckets[e.seqToBucket(seqNo)]
		e.sequences = append(e.sequences, newSequence(owner, epoch, seqNo, e.clientWindows, e.persisted, e.networkConfig, e.myConfig))
	}

	return e.drainProposer()
}

func (e *epoch) drainProposer() *Actions {
	actions := &Actions{}

	for bucketID, ownerID := range e.buckets {
		if ownerID != NodeID(e.myConfig.ID) {
			continue
		}

		for e.proposer.hasPending(bucketID) {
			i := e.lowestUnallocated[int(bucketID)]
			if i >= len(e.sequences) {
				break
			}
			seq := e.sequences[i]

			if len(e.sequences)-i <= int(e.networkConfig.CheckpointInterval) && !e.ending {
				// let the network move watermarks before filling up the last checkpoint
				// interval
				break
			}

			if ownerID == NodeID(e.myConfig.ID) && e.proposer.hasPending(bucketID) {
				// TODO, roll this back into the proposer?
				proposals := e.proposer.next(bucketID)
				requestAcks := make([]*pb.RequestAck, len(proposals))
				for i, proposal := range proposals {
					requestAcks[i] = &pb.RequestAck{
						ClientId: proposal.data.ClientId,
						ReqNo:    proposal.data.ReqNo,
						Digest:   proposal.digest,
					}
				}
				actions.Append(seq.allocate(requestAcks))
				e.lowestUnallocated[int(bucketID)] += len(e.buckets)
			}
		}
	}

	return actions
}

func (e *epoch) applyProcessResult(seqNo uint64, digest []byte) *Actions {
	seq, _, err := e.getSequence(seqNo)
	if err != nil {
		e.myConfig.Logger.Error(err.Error())
		return &Actions{}
	}

	return seq.applyProcessResult(digest)
}

func (e *epoch) tick() *Actions {
	if e.lowestUncommitted < len(e.sequences) && e.sequences[e.lowestUncommitted].seqNo != e.lastCommittedAtTick+1 {
		e.ticksSinceProgress = 0
		e.lastCommittedAtTick = e.sequences[e.lowestUncommitted].seqNo - 1
		return &Actions{}
	}

	e.ticksSinceProgress++
	actions := &Actions{}

	if e.ticksSinceProgress > e.myConfig.SuspectTicks {
		actions.Append(&Actions{
			Broadcast: []*pb.Msg{
				{
					Type: &pb.Msg_Suspect{
						Suspect: &pb.Suspect{
							Epoch: e.epochConfig.Number,
						},
					},
				},
			},
		})
	}

	if e.myConfig.HeartbeatTicks == 0 || e.ticksSinceProgress%e.myConfig.HeartbeatTicks != 0 {
		return actions
	}

	for bucketID, index := range e.lowestUnallocated {
		if index >= len(e.sequences) {
			continue
		}

		if e.buckets[BucketID(bucketID)] != NodeID(e.myConfig.ID) {
			continue
		}

		if len(e.sequences)-index <= int(e.networkConfig.CheckpointInterval) && !e.ending {
			continue
		}

		if e.proposer.hasOutstanding(BucketID(bucketID)) {
			// TODO, roll this back into the proposer?
			proposals := e.proposer.next(BucketID(bucketID))
			requestAcks := make([]*pb.RequestAck, len(proposals))
			for i, proposal := range proposals {
				requestAcks[i] = &pb.RequestAck{
					ClientId: proposal.data.ClientId,
					ReqNo:    proposal.data.ReqNo,
					Digest:   proposal.digest,
				}
			}
			actions.Append(e.sequences[index].allocate(requestAcks))
		} else {
			actions.Append(e.sequences[index].allocate(nil))
		}

		e.lowestUnallocated[int(bucketID)] += len(e.buckets)
	}

	return actions
}

func (e *epoch) lowWatermark() uint64 {
	return e.sequences[0].seqNo
}

func (e *epoch) highWatermark() uint64 {
	return e.sequences[len(e.sequences)-1].seqNo
}

func (e *epoch) status() []*BucketStatus {
	buckets := make([]*BucketStatus, len(e.buckets))
	for i := range buckets {
		bucket := &BucketStatus{
			ID:        uint64(i),
			Leader:    e.buckets[BucketID(i)] == NodeID(e.myConfig.ID),
			Sequences: make([]SequenceState, 0, len(e.sequences)/len(buckets)),
		}

		for j := i; j < len(e.sequences); j = j + len(buckets) {
			bucket.Sequences = append(bucket.Sequences, e.sequences[j].state)
		}

		buckets[i] = bucket
	}

	return buckets
}
