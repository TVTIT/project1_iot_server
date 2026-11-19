package mqttingress

import (
	"strings"
	"time"

	"github.com/google/uuid"

	"iot-platform/internal/telemetry"
)

const (
	// DedupTTL is fixed by the P5b contract and starts at confirmed commit observation.
	DedupTTL = 10 * time.Minute

	// DedupMetadataFixedOverheadBytes is a logical estimate for map/key/value
	// headers, scalar fields, and allocator/slot overhead, not Go RSS.
	DedupMetadataFixedOverheadBytes int64 = 256
)

// dedupKey is exactly the validated stream identity plus the first sequence.
type dedupKey struct {
	gatewayID     string
	sensorID      string
	bootID        string
	firstSequence int64
}

// dedupMetadata is a value-only proof record. It must never gain payload,
// sample, ACK, slice, or Message references.
type dedupMetadata struct {
	messageID         string
	payloadHash       string
	sourceTopic       string
	lastSequence      int64
	sampleCount       int
	originalTimestamp time.Time
	confirmedCommitAt time.Time
	expiresAt         time.Time
}

func buildDedupMetadata(message *telemetry.Message, confirmedCommitAt time.Time) (dedupKey, dedupMetadata, bool) {
	if message == nil || confirmedCommitAt.IsZero() {
		return dedupKey{}, dedupMetadata{}, false
	}
	topic := message.Topic()
	identity := message.Identity()
	messageID := identity.MessageID.String()
	bootID := identity.BootID.String()
	first, firstOK := message.FirstSample()
	last, lastOK := message.LastSample()
	count := message.SampleCount()
	hash := message.PayloadHash()
	if !firstOK || !lastOK || count <= 0 || identity.GatewayID == "" || identity.SensorID == "" || identity.MessageID == uuid.Nil || identity.BootID == uuid.Nil ||
		topic.SourceTopic == "" || topic.GatewayID != identity.GatewayID || topic.SensorID != identity.SensorID || len(hash) != 64 ||
		first.GatewayID != identity.GatewayID || first.SensorID != identity.SensorID || first.BootID != identity.BootID || first.MessageID != identity.MessageID ||
		last.GatewayID != identity.GatewayID || last.SensorID != identity.SensorID || last.BootID != identity.BootID || last.MessageID != identity.MessageID ||
		first.SequenceNum < 0 || last.SequenceNum < first.SequenceNum || last.SequenceNum-first.SequenceNum != int64(count-1) ||
		!strings.HasPrefix(topic.SourceTopic, "gateways/") {
		return dedupKey{}, dedupMetadata{}, false
	}
	commitAt := confirmedCommitAt.UTC()
	originalAt := first.MeasuredAt.UTC().Truncate(time.Microsecond)
	key := dedupKey{gatewayID: identity.GatewayID, sensorID: identity.SensorID, bootID: bootID, firstSequence: first.SequenceNum}
	proof := dedupMetadata{
		messageID: messageID, payloadHash: hash, sourceTopic: topic.SourceTopic,
		lastSequence: last.SequenceNum, sampleCount: count, originalTimestamp: originalAt,
		confirmedCommitAt: commitAt, expiresAt: commitAt.Add(DedupTTL),
	}
	return key, proof, true
}

func (m dedupMetadata) equal(other dedupMetadata) bool {
	return m.messageID == other.messageID && m.payloadHash == other.payloadHash && m.sourceTopic == other.sourceTopic &&
		m.lastSequence == other.lastSequence && m.sampleCount == other.sampleCount && m.originalTimestamp.Equal(other.originalTimestamp)
}

func (m dedupMetadata) logicalBytes(key dedupKey) int64 {
	parts := [...]int{len(key.gatewayID), len(key.sensorID), len(key.bootID), len(m.messageID), len(m.payloadHash), len(m.sourceTopic)}
	total := int64(0)
	for _, part := range parts {
		if uint64(part) > uint64(^uint64(0)>>1)-uint64(total) {
			return -1
		}
		total += int64(part)
	}
	if total > int64(^uint64(0)>>1)-DedupMetadataFixedOverheadBytes {
		return -1
	}
	return total + DedupMetadataFixedOverheadBytes
}
