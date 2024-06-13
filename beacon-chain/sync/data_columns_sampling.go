package sync

import (
	"context"
	"fmt"
	"sort"

	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/pkg/errors"
	"github.com/prysmaticlabs/prysm/v5/beacon-chain/core/feed"
	statefeed "github.com/prysmaticlabs/prysm/v5/beacon-chain/core/feed/state"
	"github.com/prysmaticlabs/prysm/v5/beacon-chain/core/peerdas"
	"github.com/prysmaticlabs/prysm/v5/beacon-chain/p2p"
	"github.com/prysmaticlabs/prysm/v5/beacon-chain/p2p/types"
	fieldparams "github.com/prysmaticlabs/prysm/v5/config/fieldparams"
	"github.com/prysmaticlabs/prysm/v5/config/params"
	"github.com/prysmaticlabs/prysm/v5/crypto/rand"
	eth "github.com/prysmaticlabs/prysm/v5/proto/prysm/v1alpha1"
	"github.com/prysmaticlabs/prysm/v5/runtime/version"
	"github.com/sirupsen/logrus"
)

// This const will be replaced when IncrementalDAS is implemented
// https://ethresear.ch/t/lossydas-lossy-incremental-and-diagonal-sampling-for-data-availability/18963#incrementaldas-dynamically-increase-the-sample-size-10
const allowedFailures = 1

// randomUniformIntegersWithoutReplacement returns a map of `count` random integers, without replacement in the range [0, max[.
func randomUniformIntegersWithoutReplacement(count uint64, max uint64) map[uint64]bool {
	result := make(map[uint64]bool, count)
	randGenerator := rand.NewGenerator()

	for uint64(len(result)) < count {
		n := randGenerator.Uint64() % max
		result[n] = true
	}

	return result
}

// sortedListFromMap returns a sorted list of keys from a map.
func sortedListFromMap(m map[uint64]bool) []uint64 {
	result := make([]uint64, 0, len(m))
	for k := range m {
		result = append(result, k)
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i] < result[j]
	})

	return result
}

// sampleDataColumnFromPeer samples data columns from a peer.
// It returns:
// - columns to sample minus the columns the peer should custody, and
// - columns the peer should custody but we failed to sample from.
func (s *Service) sampleDataColumnFromPeer(
	pid peer.ID,
	columnsToSample map[uint64]bool,
	requestedRoot [fieldparams.RootLength]byte,
) (map[uint64]bool, map[uint64]bool, error) {
	// Retrieve the custody count of the peer.
	peerCustodiedSubnetCount := s.cfg.p2p.CustodyCountFromRemotePeer(pid)

	// Extract the node ID from the peer ID.
	nodeID, err := p2p.ConvertPeerIDToNodeID(pid)
	if err != nil {
		return nil, nil, errors.Wrap(err, "extract node ID")
	}

	// Determine which columns the peer should custody.
	peerCustodiedColumns, err := peerdas.CustodyColumns(nodeID, peerCustodiedSubnetCount)
	if err != nil {
		return nil, nil, errors.Wrap(err, "custody columns")
	}

	peerCustodiedColumnsList := sortedListFromMap(peerCustodiedColumns)

	// Compute the intersection of the columns to sample and the columns the peer should custody.
	// Compute the columns still to be sampled after the peer has been queried.
	peerRequestedColumns := make(map[uint64]bool, len(columnsToSample))
	remainingColumns := make(map[uint64]bool, len(columnsToSample))

	for column := range columnsToSample {
		// If the peer should custody the column, we add it to peer requested columns.
		if peerCustodiedColumns[column] {
			peerRequestedColumns[column] = true
			continue
		}

		// Else, this column should be sampled from another peer.
		remainingColumns[column] = true
	}

	remainingColumnsList := sortedListFromMap(remainingColumns)
	peerRequestedColumnsList := sortedListFromMap(peerRequestedColumns)

	// Get the data column identifiers to sample from this peer.
	dataColumnIdentifiers := make(types.BlobSidecarsByRootReq, 0, len(peerRequestedColumns))
	for index := range peerRequestedColumns {
		dataColumnIdentifiers = append(dataColumnIdentifiers, &eth.BlobIdentifier{
			BlockRoot: requestedRoot[:],
			Index:     index,
		})
	}

	// Return early if there are no data columns to sample.
	if len(dataColumnIdentifiers) == 0 {
		log.WithFields(logrus.Fields{
			"peerID":           pid,
			"custodiedColumns": peerCustodiedColumnsList,
			"requestedColumns": peerRequestedColumnsList,
		}).Debug("Peer does not custody any of the requested columns")
		return columnsToSample, nil, nil
	}

	// Sample data columns.
	roDataColumns, err := SendDataColumnSidecarByRoot(s.ctx, s.cfg.clock, s.cfg.p2p, pid, s.ctxMap, &dataColumnIdentifiers)
	if err != nil {
		return nil, nil, errors.Wrap(err, "send data column sidecar by root")
	}

	peerRetrievedColumns := make(map[uint64]bool, len(roDataColumns))

	// Remove retrieved items from rootsByDataColumnIndex.
	for _, roDataColumn := range roDataColumns {
		retrievedColumn := roDataColumn.ColumnIndex

		actualRoot := roDataColumn.BlockRoot()
		if actualRoot != requestedRoot {
			// TODO: Should we decrease the peer score here?
			log.WithFields(logrus.Fields{
				"peerID":        pid,
				"requestedRoot": fmt.Sprintf("%#x", requestedRoot),
				"actualRoot":    fmt.Sprintf("%#x", actualRoot),
			}).Warning("Actual root does not match requested root")

			continue
		}

		peerRetrievedColumns[retrievedColumn] = true

		if !columnsToSample[retrievedColumn] {
			// TODO: Should we decrease the peer score here?
			log.WithFields(logrus.Fields{
				"peerID":           pid,
				"retrievedColumn":  retrievedColumn,
				"requestedColumns": peerRequestedColumnsList,
			}).Warning("Retrieved column was not requested")
		}
	}

	// Get all the requested but not retrieved columns.
	peerMissingColumns := make(map[uint64]bool, min(0, len(peerRequestedColumns)-len(peerRetrievedColumns)))
	for column := range peerRequestedColumns {
		if !peerRetrievedColumns[column] {
			peerMissingColumns[column] = true
		}
	}

	peerRetrievedColumnsList := sortedListFromMap(peerRetrievedColumns)
	peerMissingColumnsList := sortedListFromMap(peerMissingColumns)

	log.WithFields(logrus.Fields{
		"peerID":               pid,
		"peerCustodiedColumns": peerCustodiedColumnsList,
		"peerRequestedColumns": peerRequestedColumnsList,
		"peerRetrievedColumns": peerRetrievedColumnsList,
		"peerMissingColumns":   peerMissingColumnsList,
		"remainingColumns":     remainingColumnsList,
	}).Debug("Peer data column sampling summary")

	return remainingColumns, peerMissingColumns, nil
}

// sampleDataColumns samples data columns from active peers.
func (s *Service) sampleDataColumns(
	requestedRoot [fieldparams.RootLength]byte,
	samplesCount uint64,
	allowedFailuresCount uint64,
) error {
	// Determine `samplesCount` random column indexes.
	requestedColumns := randomUniformIntegersWithoutReplacement(samplesCount, params.BeaconConfig().NumberOfColumns)
	requestedColumnsList := sortedListFromMap(requestedColumns)

	missingColumns := make(map[uint64]bool, len(requestedColumns))
	for index := range requestedColumns {
		missingColumns[index] = true
	}

	// Get the active peers from the p2p service.
	activePeers := s.cfg.p2p.Peers().Active()

	var err error

	// Sampling is done sequentially peer by peer.
	// TODO: Add parallelism if (probably) needed.
	failures := make(map[uint64]bool)

	for _, pid := range activePeers {
		// Early exit the missing columns count is less than the allowed failures count.
		// This is the happy path.
		missingColumnsCount := uint64(len(missingColumns))
		if missingColumnsCount <= allowedFailuresCount {
			break
		}

		var peerFailures map[uint64]bool

		// Sample data columns from the peer.
		missingColumns, peerFailures, err = s.sampleDataColumnFromPeer(pid, missingColumns, requestedRoot)
		if err != nil {
			return errors.Wrap(err, "sample data column from peer")
		}

		// Add the peer failures to the global failures.
		for column := range peerFailures {
			failures[column] = true
		}

		// Compute the total number of failures.
		failuresCount := uint64(len(failures))

		if failuresCount > allowedFailuresCount {
			missingColumnsList := sortedListFromMap(missingColumns)

			log.WithFields(logrus.Fields{
				"requestedColumns": requestedColumnsList,
				"missingColumns":   missingColumnsList,
			}).Warning("Failed to sample some requested columns")

			return nil
		}
	}

	if len(missingColumns) > 0 {
		// All peers have been queried and some columns are still missing.
		missingColumnsList := sortedListFromMap(missingColumns)

		log.WithFields(logrus.Fields{
			"requestedColumns": requestedColumnsList,
			"missingColumns":   missingColumnsList,
		}).Warning("Failed to sample all requested columns (peers exhausted)")

		return nil
	}

	// This is the happy path.
	log.WithField("requestedColumns", requestedColumnsList).Debug("Successfully sampled all requested columns")

	return nil
}

func (s *Service) dataColumnSampling(ctx context.Context) {
	// Create a subscription to the state feed.
	stateChannel := make(chan *feed.Event, 1)
	stateSub := s.cfg.stateNotifier.StateFeed().Subscribe(stateChannel)

	// Compute the count of data columns to sample.
	samplesPerSlot := int64(params.BeaconConfig().SamplesPerSlot)
	dataColumnSamplingCountInt64, err := peerdas.ExtendedSampleCount(samplesPerSlot, allowedFailures)
	if err != nil {
		log.WithError(err).Error("Failed to compute data column sampling count")
		return
	}

	dataColumnSamplingCount := uint64(dataColumnSamplingCountInt64)

	// Unsubscribe from the state feed when the function returns.
	defer stateSub.Unsubscribe()

	for {
		select {
		case e := <-stateChannel:
			if e.Type != statefeed.BlockProcessed {
				continue
			}

			data, ok := e.Data.(*statefeed.BlockProcessedData)
			if !ok {
				log.Error("Event feed data is not of type *statefeed.BlockProcessedData")
				continue
			}

			if !data.Verified {
				// We only process blocks that have been verified
				log.Error("Data is not verified")
				continue
			}

			if data.SignedBlock.Version() < version.Deneb {
				log.Debug("Pre Deneb block, skipping data column sampling")
				continue
			}

			// Get the commitments for this block.
			commitments, err := data.SignedBlock.Block().Body().BlobKzgCommitments()
			if err != nil {
				log.WithError(err).Error("Failed to get blob KZG commitments")
				continue
			}

			// Skip if there are no commitments.
			if len(commitments) == 0 {
				log.Debug("No commitments in block, skipping data column sampling")
				continue
			}

			// Sample data columns.
			if err := s.sampleDataColumns(data.BlockRoot, dataColumnSamplingCount, allowedFailures); err != nil {
				log.WithError(err).Error("Failed to sample data columns")
			}

		case <-s.ctx.Done():
			log.Debug("Context closed, exiting goroutine")
			return

		case err := <-stateSub.Err():
			log.WithError(err).Error("Subscription to state feed failed")
		}
	}
}
