package sync

import (
	"testing"

	"github.com/prysmaticlabs/prysm/v5/testing/require"
)

func TestRandomUniformIntegersWithoutReplacement(t *testing.T) {
	const (
		count uint64 = 5
		max   uint64 = 10
	)

	// Generate random numbers
	actual := randomUniformIntegersWithoutReplacement(count, max)

	// Check if the length of the map is equal to the count
	require.Equal(t, count, uint64(len(actual)))

	// Check if the values are within the range
	for k := range actual {
		require.Equal(t, true, k < max)
	}
}
