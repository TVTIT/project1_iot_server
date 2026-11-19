package paperload

import (
	"bufio"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"syscall"
	"time"

	"iot-platform/internal/gatewaysim"
	"iot-platform/internal/telemetry"
)

const maxE4JournalMessages = 10000
const maxE4JournalBytes int64 = 64 << 20

type e4JournalRecord struct {
	Topic         string `json:"topic"`
	GatewayID     string `json:"gateway_id"`
	SensorID      string `json:"sensor_id"`
	MessageID     string `json:"message_id"`
	BootID        string `json:"boot_id"`
	FirstSequence string `json:"first_sequence"`
	SampleCount   int32  `json:"sample_count"`
	PayloadSHA256 string `json:"payload_sha256"`
	Payload       []byte `json:"payload"`
}

// E4Journal is a synchronous generator-owner journal. It is provenance for
// bounded replay only; it is not a durable outbox or power-loss guarantee.
type E4Journal struct {
	file        *os.File
	path        string
	maxMessages int
	maxBytes    int64
	count       int
	bytes       int64
	headers     []E4JournalHeader
}

// E4JournalHeader exposes safe identity/hash provenance, never payload bytes.
type E4JournalHeader struct {
	Topic         string `json:"topic"`
	GatewayID     string `json:"gateway_id"`
	SensorID      string `json:"sensor_id"`
	MessageID     string `json:"message_id"`
	BootID        string `json:"boot_id"`
	FirstSequence string `json:"first_sequence"`
	SampleCount   int32  `json:"sample_count"`
	PayloadSHA256 string `json:"payload_sha256"`
}

// E4ReplayBatch holds the original bytes and a safe header summary.
type E4ReplayBatch struct {
	Header E4JournalHeader
	Batch  gatewaysim.BuiltBatch
}

// CreateE4Journal creates exclusively, with no symlink following or overwrite.
func CreateE4Journal(path string, maxMessages int, maxBytes int64) (*E4Journal, error) {
	if !SafePrivateJournalRef(path) || maxMessages < 1 || maxMessages > maxE4JournalMessages || maxBytes < 1 || maxBytes > maxE4JournalBytes {
		return nil, errors.New("e4_journal_settings_rejected")
	}
	full, err := filepath.Abs(path)
	if err != nil {
		return nil, errors.New("e4_journal_path_rejected")
	}
	parent := filepath.Dir(full)
	if err := checkPrivateDirectory(parent); err != nil {
		return nil, err
	}
	fd, err := syscall.Open(full, syscall.O_WRONLY|syscall.O_CREAT|syscall.O_EXCL|syscall.O_NOFOLLOW|syscall.O_CLOEXEC, 0600)
	if err != nil {
		return nil, errors.New("e4_journal_create_failed")
	}
	f := os.NewFile(uintptr(fd), full)
	if err := checkPrivateRegularFile(f); err != nil {
		_ = f.Close()
		return nil, err
	}
	return &E4Journal{file: f, path: full, maxMessages: maxMessages, maxBytes: maxBytes}, nil
}

// Append writes one exact BuiltBatch only when both capacities permit it.
func (j *E4Journal) Append(batch gatewaysim.BuiltBatch) error {
	if j == nil || j.file == nil {
		return errors.New("e4_journal_closed")
	}
	if j.count >= j.maxMessages {
		return errors.New("e4_journal_message_cap")
	}
	hash := sha256.Sum256(batch.Payload)
	b := batch.Batch
	record := e4JournalRecord{Topic: batch.Topic, GatewayID: b.GatewayID, SensorID: b.SensorID, MessageID: b.MessageID.String(), BootID: b.BootID.String(), FirstSequence: strconv.FormatInt(b.FirstSequence, 10), SampleCount: b.SampleCount, PayloadSHA256: hex.EncodeToString(hash[:]), Payload: batch.Payload}
	data, err := json.Marshal(record)
	if err != nil {
		return errors.New("e4_journal_encode_failed")
	}
	data = append(data, '\n')
	if int64(len(data)) > j.maxBytes-j.bytes {
		return errors.New("e4_journal_byte_cap")
	}
	n, err := j.file.Write(data)
	if err != nil || n != len(data) {
		return errors.New("e4_journal_write_failed")
	}
	j.count++
	j.bytes += int64(n)
	j.headers = append(j.headers, E4JournalHeader{Topic: record.Topic, GatewayID: record.GatewayID, SensorID: record.SensorID, MessageID: record.MessageID, BootID: record.BootID, FirstSequence: record.FirstSequence, SampleCount: record.SampleCount, PayloadSHA256: record.PayloadSHA256})
	return nil
}

// Headers returns a bounded copy of safe journal provenance.
func (j *E4Journal) Headers() []E4JournalHeader {
	if j == nil {
		return nil
	}
	return append([]E4JournalHeader(nil), j.headers...)
}

// Close closes the journal without removing or rewriting it.
func (j *E4Journal) Close() error {
	if j == nil || j.file == nil {
		return nil
	}
	err := j.file.Close()
	j.file = nil
	if err != nil {
		return errors.New("e4_journal_close_failed")
	}
	return nil
}

// ReadE4Journal loads only the requested prefix, bounded by manifest limits,
// and validates original payload bytes with the ingress telemetry validator.
func ReadE4Journal(path string, maxMessages int, maxBytes int64, replayCount int, targets []Target) ([]E4ReplayBatch, error) {
	if !SafePrivateJournalRef(path) || maxMessages < 1 || maxMessages > maxE4JournalMessages || maxBytes < 1 || maxBytes > maxE4JournalBytes || replayCount < 1 || replayCount > maxMessages {
		return nil, errors.New("e4_journal_settings_rejected")
	}
	full, err := filepath.Abs(path)
	if err != nil {
		return nil, errors.New("e4_journal_path_rejected")
	}
	if err = checkPrivateDirectory(filepath.Dir(full)); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(full, os.O_RDONLY|syscall.O_NOFOLLOW, 0)
	if err != nil {
		return nil, errors.New("e4_journal_read_failed")
	}
	defer func() { _ = f.Close() }()
	if err = checkPrivateRegularFile(f); err != nil {
		return nil, err
	}
	st, err := f.Stat()
	if err != nil || st.Size() > maxBytes {
		return nil, errors.New("e4_journal_byte_cap")
	}
	allow := map[string]bool{}
	for _, t := range targets {
		allow[t.GatewayID+"\x00"+t.SensorID] = true
	}
	v, err := telemetry.NewValidator(gatewaysim.ServerLimits())
	if err != nil {
		return nil, errors.New("e4_journal_validator_unavailable")
	}
	scanner := bufio.NewScanner(io.LimitReader(f, maxBytes+1))
	scanner.Buffer(make([]byte, 4096), int(maxBytes)+1)
	out := make([]E4ReplayBatch, 0, replayCount)
	hashes := map[string]string{}
	for scanner.Scan() {
		if len(out) >= replayCount {
			break
		}
		var r e4JournalRecord
		if err := json.Unmarshal(scanner.Bytes(), &r); err != nil {
			return nil, errors.New("e4_journal_record_invalid")
		}
		h := sha256.Sum256(r.Payload)
		hash := hex.EncodeToString(h[:])
		if r.PayloadSHA256 != hash {
			return nil, errors.New("e4_journal_hash_mismatch")
		}
		msg, err := v.Validate(r.Topic, r.Payload, time.Now().UTC())
		if err != nil {
			return nil, errors.New("e4_journal_payload_invalid")
		}
		b := msg.Batch()
		topic := msg.Topic()
		seq, seqErr := strconv.ParseInt(r.FirstSequence, 10, 64)
		if seqErr != nil || topic.GatewayID != r.GatewayID || topic.SensorID != r.SensorID || b.GatewayID != r.GatewayID || b.SensorID != r.SensorID || b.MessageID.String() != r.MessageID || b.BootID.String() != r.BootID || b.FirstSequence != seq || b.SampleCount != r.SampleCount || !allow[r.GatewayID+"\x00"+r.SensorID] || r.Topic != "gateways/"+r.GatewayID+"/telemetry/"+r.SensorID {
			return nil, errors.New("e4_journal_metadata_mismatch")
		}
		if prior, ok := hashes[r.MessageID]; ok && prior != hash {
			return nil, errors.New("e4_journal_duplicate_id_conflict")
		}
		if _, duplicate := hashes[r.MessageID]; duplicate {
			return nil, errors.New("e4_journal_duplicate_id")
		}
		hashes[r.MessageID] = hash
		batch := gatewaysim.BuiltBatch{Topic: r.Topic, Payload: append([]byte(nil), r.Payload...), Batch: b}
		header := E4JournalHeader{r.Topic, r.GatewayID, r.SensorID, r.MessageID, r.BootID, r.FirstSequence, r.SampleCount, hash}
		out = append(out, E4ReplayBatch{Header: header, Batch: batch})
	}
	if err := scanner.Err(); err != nil {
		return nil, errors.New("e4_journal_read_failed")
	}
	if len(out) != replayCount {
		return nil, fmt.Errorf("e4_journal_prefix_incomplete")
	}
	return out, nil
}

func checkPrivateDirectory(path string) error {
	st, err := os.Lstat(path)
	if err != nil || !st.IsDir() || st.Mode()&os.ModeSymlink != 0 || st.Mode().Perm() != 0700 || !ownedByCurrentUser(st) {
		return errors.New("e4_journal_parent_not_private")
	}
	return nil
}
func checkPrivateRegularFile(f *os.File) error {
	st, err := f.Stat()
	if err != nil || !st.Mode().IsRegular() || st.Mode().Perm() != 0600 || !ownedByCurrentUser(st) {
		return errors.New("e4_journal_file_not_private")
	}
	return nil
}
func ownedByCurrentUser(st os.FileInfo) bool {
	sys, ok := st.Sys().(*syscall.Stat_t)
	return ok && int(sys.Uid) == os.Getuid()
}
