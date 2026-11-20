package mediaworker

import (
	"bytes"
	"crypto/sha256"
	_ "embed"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"io"
	"strings"
	"testing"
)

//go:embed testdata/h264-16x16.mp4
var validH264MP4 []byte

func validMetadata(body []byte) ContentMetadata {
	sum := sha256.Sum256(body)
	return ContentMetadata{DeclaredLength: int64(len(body)), DeclaredSHA256: hex.EncodeToString(sum[:]), ServerMIME: "video/mp4"}
}

func TestVerifyContentAcceptsFinalizedH264MP4AndMeasuresBytes(t *testing.T) {
	for _, body := range [][]byte{validH264MP4, moveMoovAfterMdat(validH264MP4)} {
		got, err := VerifyContent(bytes.NewReader(body), validMetadata(body))
		if err != nil {
			t.Fatal(err)
		}
		meta := validMetadata(body)
		if got.ActualSize != int64(len(body)) || got.SHA256 != meta.DeclaredSHA256 || got.MediaType != "video/mp4" {
			t.Fatalf("evidence = %+v", got)
		}
	}
}

func TestVerifyContentRejectsMetadataAndMalformedContainers(t *testing.T) {
	mutations := []struct {
		name string
		body []byte
		meta func([]byte) ContentMetadata
		code string
	}{
		{"wrong size", validH264MP4, func(b []byte) ContentMetadata { m := validMetadata(b); m.DeclaredLength++; return m }, "SIZE_MISMATCH"},
		{"wrong hash", validH264MP4, func(b []byte) ContentMetadata {
			m := validMetadata(b)
			m.DeclaredSHA256 = strings.Repeat("a", 64)
			return m
		}, "HASH_MISMATCH"},
		{"wrong mime", validH264MP4, func(b []byte) ContentMetadata {
			m := validMetadata(b)
			m.ServerMIME = "application/octet-stream"
			return m
		}, "CONTENT_TYPE_MISMATCH"},
		{"ftyp only", append([]byte(nil), validH264MP4[:32]...), validMetadata, "INVALID_MP4"},
		{"wrong track", corruptHandlerType(validH264MP4), validMetadata, "INVALID_MP4"},
		{"truncated", validH264MP4[:len(validH264MP4)-3], validMetadata, "INVALID_MP4"},
	}
	for _, tc := range mutations {
		t.Run(tc.name, func(t *testing.T) {
			_, err := VerifyContent(bytes.NewReader(tc.body), tc.meta(tc.body))
			var invalid *ContentError
			if !errors.As(err, &invalid) || invalid.Code != tc.code {
				t.Fatalf("error = %v, want terminal code %q", err, tc.code)
			}
			if invalid.Retryable {
				t.Fatalf("complete malformed content marked retryable: %+v", invalid)
			}
		})
	}
}

func TestVerifyContentDetectsByteCapPlusOneDuringStreaming(t *testing.T) {
	ftypSize := int(binary.BigEndian.Uint32(validH264MP4[:4]))
	ftyp := validH264MP4[:ftypSize]
	md := append([]byte(nil), ftyp...)
	md = append(md, 0, 0, 0, 0, 'm', 'd', 'a', 't')
	binary.BigEndian.PutUint32(md[len(ftyp):len(ftyp)+4], uint32(MaxVerifiedBytes))
	reader := io.MultiReader(bytes.NewReader(md), io.LimitReader(zeroReader{}, MaxVerifiedBytes))
	meta := ContentMetadata{DeclaredLength: MaxVerifiedBytes, DeclaredSHA256: strings.Repeat("0", 64), ServerMIME: "video/mp4"}
	_, err := VerifyContent(reader, meta)
	var contentErr *ContentError
	if !errors.As(err, &contentErr) || contentErr.Code != "SIZE_MISMATCH" || contentErr.Retryable {
		t.Fatalf("cap+1 result = %v", err)
	}
}

func TestVerifyContentRejectsMetadataAboveByteCapBeforeReading(t *testing.T) {
	reader := &countingReader{}
	meta := ContentMetadata{
		DeclaredLength: MaxVerifiedBytes + 1,
		DeclaredSHA256: strings.Repeat("0", 64),
		ServerMIME:     "video/mp4",
	}
	_, err := VerifyContent(reader, meta)
	var contentErr *ContentError
	if !errors.As(err, &contentErr) || contentErr.Code != "SIZE_MISMATCH" || reader.reads != 0 {
		t.Fatalf("above-cap metadata result = %v, reader calls = %d", err, reader.reads)
	}
}

type countingReader struct{ reads int }

func (r *countingReader) Read([]byte) (int, error) {
	r.reads++
	return 0, io.EOF
}

func TestVerifyContentDistinguishesStreamFailureAsRetryable(t *testing.T) {
	_, err := VerifyContent(&failingReader{data: validH264MP4[:40]}, validMetadata(validH264MP4))
	var contentErr *ContentError
	if !errors.As(err, &contentErr) || !contentErr.Retryable || contentErr.Code != "STORAGE_UNAVAILABLE" {
		t.Fatalf("stream error = %v, want retryable STORAGE_UNAVAILABLE", err)
	}
}

type failingReader struct{ data []byte }

func (r *failingReader) Read(p []byte) (int, error) {
	if len(r.data) == 0 {
		return 0, errors.New("fixture transport broke")
	}
	n := copy(p, r.data)
	r.data = r.data[n:]
	return n, nil
}

type zeroReader struct{}

func (zeroReader) Read(p []byte) (int, error) {
	clear(p)
	return len(p), nil
}

func corruptHandlerType(src []byte) []byte {
	out := append([]byte(nil), src...)
	if i := bytes.Index(out, []byte("vide")); i >= 0 {
		copy(out[i:i+4], "soun")
	}
	return out
}

func moveMoovAfterMdat(src []byte) []byte {
	var ftyp, moov, other []byte
	for len(src) > 0 {
		size := int(binary.BigEndian.Uint32(src[:4]))
		box := src[:size]
		switch string(box[4:8]) {
		case "ftyp":
			ftyp = append(ftyp, box...)
		case "moov":
			moov = append(moov, box...)
		default:
			other = append(other, box...)
		}
		src = src[size:]
	}
	delta := uint32(len(moov))
	if idx := bytes.Index(moov, []byte("stco")); idx >= 4 {
		off := binary.BigEndian.Uint32(moov[idx+12 : idx+16])
		if off >= delta {
			binary.BigEndian.PutUint32(moov[idx+12:idx+16], off-delta)
		}
	}
	return append(append(ftyp, other...), moov...)
}

func TestVerifyContentRejectsEmptyMdatAndOrphanAVCStructures(t *testing.T) {
	// Positive controls: ordinary finalized H.264 fixture and moov-after-mdat order
	for _, control := range [][]byte{validH264MP4, moveMoovAfterMdat(validH264MP4)} {
		got, err := VerifyContent(bytes.NewReader(control), validMetadata(control))
		if err != nil {
			t.Fatalf("positive control failed: %v", err)
		}
		if got.ActualSize != int64(len(control)) {
			t.Fatalf("positive control size mismatch: %d != %d", got.ActualSize, len(control))
		}
	}

	tests := []struct {
		name string
		body []byte
	}{
		{
			name: "empty EOF mdat without payload",
			body: makeEmptyEOFmdatMP4(validH264MP4),
		},
		{
			name: "no declared AVC video samples",
			body: makeNoAVCSamplesMP4(validH264MP4),
		},
		{
			name: "inconsistent sample table range not in actual mdat",
			body: makeInconsistentSampleRangeMP4(validH264MP4),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			meta := validMetadata(tt.body)
			evidence, err := VerifyContent(bytes.NewReader(tt.body), meta)
			if err == nil {
				t.Fatalf("expected INVALID_MP4 for %s, but VerifyContent succeeded: %+v", tt.name, evidence)
			}
			var contentErr *ContentError
			if !errors.As(err, &contentErr) || contentErr.Code != "INVALID_MP4" {
				t.Fatalf("expected ContentError with code INVALID_MP4, got %v", err)
			}
			if contentErr.Retryable {
				t.Fatalf("malformed content must not be retryable: %+v", contentErr)
			}
		})
	}
}

func makeEmptyEOFmdatMP4(src []byte) []byte {
	var out []byte
	for len(src) > 0 {
		size := int(binary.BigEndian.Uint32(src[:4]))
		box := src[:size]
		typ := string(box[4:8])
		if typ == "ftyp" || typ == "moov" {
			out = append(out, box...)
		}
		src = src[size:]
	}
	return append(out, 0, 0, 0, 0, 'm', 'd', 'a', 't')
}

func makeNoAVCSamplesMP4(src []byte) []byte {
	out := append([]byte(nil), src...)
	idx := bytes.Index(out, []byte("stsz"))
	if idx >= 4 {
		binary.BigEndian.PutUint32(out[idx+8:idx+12], 0)
		binary.BigEndian.PutUint32(out[idx+12:idx+16], 0)
	}
	return out
}

func makeInconsistentSampleRangeMP4(src []byte) []byte {
	out := append([]byte(nil), src...)
	idx := bytes.Index(out, []byte("stco"))
	if idx >= 4 {
		binary.BigEndian.PutUint32(out[idx+12:idx+16], 0)
	}
	return out
}

func TestVerifyContentTableBoundsAndProfileValidation(t *testing.T) {
	t.Run("positive_control_co64", func(t *testing.T) {
		co64MP4 := makeCo64MP4(validH264MP4)
		got, err := VerifyContent(bytes.NewReader(co64MP4), validMetadata(co64MP4))
		if err != nil {
			t.Fatalf("positive control co64 failed: %v", err)
		}
		if got.ActualSize != int64(len(co64MP4)) {
			t.Fatalf("co64 size mismatch: %d != %d", got.ActualSize, len(co64MP4))
		}
	})

	t.Run("stsc_non_monotonic_first_chunk", func(t *testing.T) {
		nonMonoStsc := makeMalformedStscNonMonotonicMP4(validH264MP4)
		if _, err := VerifyContent(bytes.NewReader(nonMonoStsc), validMetadata(nonMonoStsc)); err == nil {
			t.Fatal("expected rejection for non-monotonic stsc first_chunk, got nil")
		}
	})

	t.Run("stsz_excessive_sample_count", func(t *testing.T) {
		stszOver := makeMalformedStszExcessiveCountMP4(validH264MP4)
		if _, err := VerifyContent(bytes.NewReader(stszOver), validMetadata(stszOver)); err == nil {
			t.Fatal("expected rejection for stsz count exceeding physical table capacity, got nil")
		}
	})

	t.Run("stco_excessive_chunk_count", func(t *testing.T) {
		stcoOver := makeMalformedStcoExcessiveCountMP4(validH264MP4)
		if _, err := VerifyContent(bytes.NewReader(stcoOver), validMetadata(stcoOver)); err == nil {
			t.Fatal("expected rejection for stco count exceeding physical table capacity, got nil")
		}
	})

	t.Run("fragmented_mp4_moof_box", func(t *testing.T) {
		moofMP4 := makeMoofMP4(validH264MP4)
		if _, err := VerifyContent(bytes.NewReader(moofMP4), validMetadata(moofMP4)); err == nil {
			t.Fatal("expected rejection for fragmented MP4 (moof box present), got nil")
		}
	})
}

func makeCo64MP4(src []byte) []byte {
	out := append([]byte(nil), src...)
	stcoIdx := bytes.Index(out, []byte("stco"))
	if stcoIdx < 4 {
		return src
	}
	origOff := binary.BigEndian.Uint32(out[stcoIdx+12 : stcoIdx+16])
	newOff := uint64(origOff) + 4

	co64 := make([]byte, 24)
	binary.BigEndian.PutUint32(co64[:4], 24)
	copy(co64[4:8], "co64")
	binary.BigEndian.PutUint32(co64[8:12], 0)
	binary.BigEndian.PutUint32(co64[12:16], 1)
	binary.BigEndian.PutUint64(co64[16:24], newOff)

	boxStart := stcoIdx - 4
	stcoLen := int(binary.BigEndian.Uint32(out[boxStart : boxStart+4]))
	res := append(append([]byte(nil), out[:boxStart]...), co64...)
	res = append(res, out[boxStart+stcoLen:]...)

	for _, typ := range []string{"moov", "trak", "mdia", "minf", "stbl"} {
		idx := bytes.Index(res, []byte(typ))
		if idx >= 4 {
			sz := binary.BigEndian.Uint32(res[idx-4 : idx])
			binary.BigEndian.PutUint32(res[idx-4:idx], sz+4)
		}
	}
	return res
}

func makeMalformedStscNonMonotonicMP4(src []byte) []byte {
	out := append([]byte(nil), src...)
	idx := bytes.Index(out, []byte("stsc"))
	if idx >= 4 {
		// Set entry_count = 2, append second record with first_chunk = 1 (non-monotonic)
		binary.BigEndian.PutUint32(out[idx+8:idx+12], 2)
		// We insert 12 bytes for second record
		secondRecord := []byte{0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0, 1}
		insertPos := idx + 24
		res := append(append([]byte(nil), out[:insertPos]...), secondRecord...)
		res = append(res, out[insertPos:]...)
		// Expand sizes of stsc, stbl, minf, mdia, trak, moov by 12
		stscSz := binary.BigEndian.Uint32(res[idx-4 : idx])
		binary.BigEndian.PutUint32(res[idx-4:idx], stscSz+12)
		for _, typ := range []string{"moov", "trak", "mdia", "minf", "stbl"} {
			bIdx := bytes.Index(res, []byte(typ))
			if bIdx >= 4 {
				sz := binary.BigEndian.Uint32(res[bIdx-4 : bIdx])
				binary.BigEndian.PutUint32(res[bIdx-4:bIdx], sz+12)
			}
		}
		// Adjust stco chunk offset by +12
		if stcoIdx := bytes.Index(res, []byte("stco")); stcoIdx >= 4 {
			cOff := binary.BigEndian.Uint32(res[stcoIdx+12 : stcoIdx+16])
			binary.BigEndian.PutUint32(res[stcoIdx+12:stcoIdx+16], cOff+12)
		}
		return res
	}
	return out
}

func makeMalformedStszExcessiveCountMP4(src []byte) []byte {
	out := append([]byte(nil), src...)
	idx := bytes.Index(out, []byte("stsz"))
	if idx >= 4 {
		// Set default_sample_size = 0, sample_count = 50 (while physical table only has 1 entry)
		binary.BigEndian.PutUint32(out[idx+8:idx+12], 0)
		binary.BigEndian.PutUint32(out[idx+12:idx+16], 50)
		oneSample := []byte{0, 0, 0x02, 0xc5}
		insertPos := idx + 16
		res := append(append([]byte(nil), out[:insertPos]...), oneSample...)
		res = append(res, out[insertPos:]...)
		stszSz := binary.BigEndian.Uint32(res[idx-4 : idx])
		binary.BigEndian.PutUint32(res[idx-4:idx], stszSz+4)
		for _, typ := range []string{"moov", "trak", "mdia", "minf", "stbl"} {
			bIdx := bytes.Index(res, []byte(typ))
			if bIdx >= 4 {
				sz := binary.BigEndian.Uint32(res[bIdx-4 : bIdx])
				binary.BigEndian.PutUint32(res[bIdx-4:bIdx], sz+4)
			}
		}
		if stcoIdx := bytes.Index(res, []byte("stco")); stcoIdx >= 4 {
			cOff := binary.BigEndian.Uint32(res[stcoIdx+12 : stcoIdx+16])
			binary.BigEndian.PutUint32(res[stcoIdx+12:stcoIdx+16], cOff+4)
		}
		return res
	}
	return out
}

func makeMalformedStcoExcessiveCountMP4(src []byte) []byte {
	out := append([]byte(nil), src...)
	idx := bytes.Index(out, []byte("stco"))
	if idx >= 4 {
		// chunk_count = 50 (physical table only has 1 entry)
		binary.BigEndian.PutUint32(out[idx+8:idx+12], 50)
	}
	return out
}

func makeMoofMP4(src []byte) []byte {
	out := append([]byte(nil), src...)
	// Append top-level moof box (8 bytes)
	moof := []byte{0, 0, 0, 8, 'm', 'o', 'o', 'f'}
	return append(out, moof...)
}
