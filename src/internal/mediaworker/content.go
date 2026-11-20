package mediaworker

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"hash"
	"io"
	"math"
	"strings"
)

const (
	// MaxVerifiedBytes is the fixed admission cap for managed media uploads.
	MaxVerifiedBytes    int64 = 80_000_000
	maxFTYPBytes              = 1 << 20
	maxMOOVBytes              = 8 << 20
	maxMdatBoxes              = 16
	maxCandidateSamples       = 64
)

// ContentMetadata describes the committed intent and the server-observed
// Storage response. No request-controlled object path is accepted here.
type ContentMetadata struct {
	DeclaredLength int64
	DeclaredSHA256 string
	ServerMIME     string
}

// ContentEvidence contains values measured from the complete byte stream.
type ContentEvidence struct {
	ActualSize int64
	SHA256     string
	MediaType  string
}

// ContentError represents an error verifying media content where Code is safe to persist or return.
// Retryable is true only for an I/O failure reading the Storage response, never for malformed bytes.
type ContentError struct {
	Code      string
	Retryable bool
}

func (e *ContentError) Error() string { return e.Code }

// VerifyContent inspects the stream to verify MP4 structure, box constraints, size, and hash against metadata.
func VerifyContent(src io.Reader, metadata ContentMetadata) (ContentEvidence, error) {
	if src == nil {
		return ContentEvidence{}, &ContentError{Code: "STORAGE_UNAVAILABLE", Retryable: true}
	}
	if metadata.ServerMIME != "video/mp4" {
		return ContentEvidence{}, &ContentError{Code: "CONTENT_TYPE_MISMATCH"}
	}
	if metadata.DeclaredLength < 0 || metadata.DeclaredLength > MaxVerifiedBytes {
		return ContentEvidence{}, &ContentError{Code: "SIZE_MISMATCH"}
	}
	if len(metadata.DeclaredSHA256) != sha256.Size*2 || strings.ToLower(metadata.DeclaredSHA256) != metadata.DeclaredSHA256 {
		return ContentEvidence{}, &ContentError{Code: "HASH_MISMATCH"}
	}
	if _, err := hex.DecodeString(metadata.DeclaredSHA256); err != nil {
		return ContentEvidence{}, &ContentError{Code: "HASH_MISMATCH"}
	}

	stream := &boundedDigestReader{r: src, h: sha256.New(), limit: MaxVerifiedBytes}
	var haveFTYP, haveMOOV, haveMDAT, haveAVC bool
	var mdatRanges []byteRange
	var avcSampleRanges []byteRange
scanLoop:
	for {
		start := stream.n
		var header [8]byte
		n, err := io.ReadFull(stream, header[:])
		if err == io.EOF && n == 0 {
			break
		}
		if err != nil {
			return ContentEvidence{}, classifyStreamError(stream, err)
		}
		boxSize := uint64(binary.BigEndian.Uint32(header[:4]))
		boxType := string(header[4:])
		headerSize := uint64(8)
		switch boxSize {
		case 1:
			var extended [8]byte
			if _, err = io.ReadFull(stream, extended[:]); err != nil {
				return ContentEvidence{}, classifyStreamError(stream, err)
			}
			boxSize = binary.BigEndian.Uint64(extended[:])
			headerSize = 16
		case 0:
			// Size zero extends to EOF. Only the final mdat is useful here.
			if boxType != "mdat" {
				return ContentEvidence{}, invalidMP4()
			}
			boxSize = math.MaxUint64
		}
		if boxSize < headerSize {
			return ContentEvidence{}, invalidMP4()
		}
		payloadSize := boxSize - headerSize
		if boxSize == math.MaxUint64 {
			payloadSize = uint64(MaxVerifiedBytes) + 1
		}
		switch boxType {
		case "moof":
			// Fragmented MP4 is not supported in this finalized profile.
			return ContentEvidence{}, invalidMP4()
		case "ftyp":
			if haveFTYP || payloadSize > maxFTYPBytes {
				return ContentEvidence{}, invalidMP4()
			}
			payload := make([]byte, int(payloadSize))
			if _, err = io.ReadFull(stream, payload); err != nil {
				return ContentEvidence{}, classifyStreamError(stream, err)
			}
			if !validFTYP(payload) {
				return ContentEvidence{}, invalidMP4()
			}
			haveFTYP = true
		case "moov":
			if haveMOOV || payloadSize > maxMOOVBytes {
				return ContentEvidence{}, invalidMP4()
			}
			payload := make([]byte, int(payloadSize))
			if _, err = io.ReadFull(stream, payload); err != nil {
				return ContentEvidence{}, classifyStreamError(stream, err)
			}
			samples, ok := parseAVCTrackSamples(payload)
			if ok && len(samples) > 0 {
				haveAVC = true
				avcSampleRanges = append(avcSampleRanges, samples...)
			}
			haveMOOV = true
		case "mdat":
			if len(mdatRanges) >= maxMdatBoxes {
				return ContentEvidence{}, invalidMP4()
			}
			payloadStart := stream.n
			if boxSize == math.MaxUint64 {
				// Drain to EOF with the global cap; no file-sized allocation.
				drained, err := io.Copy(io.Discard, stream)
				if err != nil {
					return ContentEvidence{}, classifyStreamError(stream, err)
				}
				payloadEnd := payloadStart + drained
				if payloadEnd <= payloadStart {
					return ContentEvidence{}, invalidMP4()
				}
				haveMDAT = true
				mdatRanges = append(mdatRanges, byteRange{start: payloadStart, end: payloadEnd})
				break scanLoop
			}
			if payloadSize == 0 || payloadSize > math.MaxInt64 {
				return ContentEvidence{}, invalidMP4()
			}
			payloadEnd := payloadStart + int64(payloadSize)
			if payloadEnd <= payloadStart {
				return ContentEvidence{}, invalidMP4()
			}
			haveMDAT = true
			mdatRanges = append(mdatRanges, byteRange{start: payloadStart, end: payloadEnd})
			if _, err = io.CopyN(io.Discard, stream, int64(payloadSize)); err != nil {
				return ContentEvidence{}, classifyStreamError(stream, err)
			}
		default:
			if boxSize == math.MaxUint64 {
				_, err = io.Copy(io.Discard, stream)
				if err != nil {
					return ContentEvidence{}, classifyStreamError(stream, err)
				}
				break scanLoop
			}
			if payloadSize > math.MaxInt64 {
				return ContentEvidence{}, invalidMP4()
			}
			if _, err = io.CopyN(io.Discard, stream, int64(payloadSize)); err != nil {
				return ContentEvidence{}, classifyStreamError(stream, err)
			}
		}
		if stream.n <= start {
			return ContentEvidence{}, invalidMP4()
		}
	}
	digest := hex.EncodeToString(stream.h.Sum(nil))
	if stream.n > MaxVerifiedBytes || stream.n != metadata.DeclaredLength {
		return ContentEvidence{}, &ContentError{Code: "SIZE_MISMATCH"}
	}
	if digest != metadata.DeclaredSHA256 {
		return ContentEvidence{}, &ContentError{Code: "HASH_MISMATCH"}
	}
	if !haveFTYP || !haveMOOV || !haveMDAT || !haveAVC {
		return ContentEvidence{}, invalidMP4()
	}
	var sampleBacked bool
	for _, s := range avcSampleRanges {
		for _, m := range mdatRanges {
			if s.start >= m.start && s.end <= m.end {
				sampleBacked = true
				break
			}
		}
		if sampleBacked {
			break
		}
	}
	if !sampleBacked {
		return ContentEvidence{}, invalidMP4()
	}
	return ContentEvidence{ActualSize: stream.n, SHA256: digest, MediaType: "video/mp4"}, nil
}

type boundedDigestReader struct {
	r       io.Reader
	h       hash.Hash
	n       int64
	limit   int64
	readErr error
}

func (r *boundedDigestReader) Read(p []byte) (int, error) {
	if r.n > r.limit {
		return 0, errContentTooLarge
	}
	remaining := r.limit + 1 - r.n
	if int64(len(p)) > remaining {
		p = p[:remaining]
	}
	n, err := r.r.Read(p)
	if err != nil && !errors.Is(err, io.EOF) {
		r.readErr = err
	}
	if n > 0 {
		r.n += int64(n)
		_, _ = r.h.Write(p[:n])
	}
	if r.n > r.limit {
		return n, errContentTooLarge
	}
	return n, err
}

var errContentTooLarge = errors.New("content exceeds byte cap")

func classifyStreamError(stream *boundedDigestReader, err error) *ContentError {
	if errors.Is(err, errContentTooLarge) {
		return &ContentError{Code: "SIZE_MISMATCH"}
	}
	if stream.readErr != nil {
		return &ContentError{Code: "STORAGE_UNAVAILABLE", Retryable: true}
	}
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		return invalidMP4()
	}
	return &ContentError{Code: "STORAGE_UNAVAILABLE", Retryable: true}
}

func invalidMP4() *ContentError { return &ContentError{Code: "INVALID_MP4"} }

func validFTYP(data []byte) bool {
	if len(data) < 8 || (len(data)-8)%4 != 0 {
		return false
	}
	known := func(b []byte) bool {
		switch string(b) {
		case "isom", "iso2", "mp41", "mp42", "avc1", "M4V ", "qt  ":
			return true
		}
		return false
	}
	if known(data[:4]) {
		return true
	}
	for i := 8; i+4 <= len(data); i += 4 {
		if known(data[i : i+4]) {
			return true
		}
	}
	return false
}

type bmffBox struct {
	typ  string
	data []byte
}

func boxes(data []byte) ([]bmffBox, bool) {
	var out []bmffBox
	for len(data) > 0 {
		if len(data) < 8 {
			return nil, false
		}
		size := uint64(binary.BigEndian.Uint32(data[:4]))
		header := uint64(8)
		switch size {
		case 1:
			if len(data) < 16 {
				return nil, false
			}
			size = binary.BigEndian.Uint64(data[8:16])
			header = 16
		case 0:
			return nil, false
		}
		if size < header || size > uint64(len(data)) {
			return nil, false
		}
		out = append(out, bmffBox{typ: string(data[4:8]), data: data[header:size]})
		data = data[size:]
	}
	return out, true
}

type byteRange struct {
	start int64
	end   int64
}

// parseAVCTrackSamples inspects the moov box for an AVC video track (hdlr == "vide")
// and returns all referenced candidate sample ranges [start, end) derived from
// the track's sample tables (stsd, stsc, stsz, stco/co64).
func parseAVCTrackSamples(moov []byte) ([]byteRange, bool) {
	top, ok := boxes(moov)
	if !ok {
		return nil, false
	}
	var allRanges []byteRange
	for _, b := range top {
		if b.typ != "trak" {
			continue
		}
		trak, ok := boxes(b.data)
		if !ok {
			continue
		}
		var isVideo bool
		var stblBoxes []bmffBox
		for _, tb := range trak {
			if tb.typ != "mdia" {
				continue
			}
			mdia, valid := boxes(tb.data)
			if !valid {
				continue
			}
			for _, mb := range mdia {
				switch mb.typ {
				case "hdlr":
					if len(mb.data) >= 12 && string(mb.data[8:12]) == "vide" {
						isVideo = true
					}
				case "minf":
					minf, valid := boxes(mb.data)
					if !valid {
						continue
					}
					for _, fb := range minf {
						if fb.typ == "stbl" {
							stbl, valid := boxes(fb.data)
							if valid {
								stblBoxes = stbl
							}
						}
					}
				}
			}
		}
		if !isVideo || len(stblBoxes) == 0 {
			continue
		}
		ranges, found := extractAVCSamplesFromStbl(stblBoxes)
		if found && len(ranges) > 0 {
			allRanges = append(allRanges, ranges...)
		}
	}
	return allRanges, len(allRanges) > 0
}

func extractAVCSamplesFromStbl(stbl []bmffBox) ([]byteRange, bool) {
	var stsdData, stscData, stszData, stcoData, co64Data []byte
	for _, b := range stbl {
		switch b.typ {
		case "stsd":
			stsdData = b.data
		case "stsc":
			stscData = b.data
		case "stsz":
			stszData = b.data
		case "stz2":
			// stz2 is deliberately excluded in this finalized profile
			return nil, false
		case "stco":
			stcoData = b.data
		case "co64":
			co64Data = b.data
		}
	}
	if len(stsdData) < 8 || len(stscData) < 8 || len(stszData) < 12 || (len(stcoData) < 8 && len(co64Data) < 8) {
		return nil, false
	}
	avcDescIndex := findAVCSampleDescIndex(stsdData)
	if avcDescIndex == 0 {
		return nil, false
	}
	stsdCount := binary.BigEndian.Uint32(stsdData[4:8])

	defSampleSize := binary.BigEndian.Uint32(stszData[4:8])
	sampleCount := binary.BigEndian.Uint32(stszData[8:12])
	if sampleCount == 0 {
		return nil, false
	}
	if defSampleSize == 0 {
		maxPhysicalSamples := uint32((len(stszData) - 12) / 4)
		if sampleCount > maxPhysicalSamples {
			return nil, false
		}
	}
	getSampleSize := func(s uint32) (uint64, bool) {
		if s < 1 || s > sampleCount {
			return 0, false
		}
		if defSampleSize > 0 {
			return uint64(defSampleSize), true
		}
		off := 12 + int(s-1)*4
		if off+4 > len(stszData) {
			return 0, false
		}
		sz := binary.BigEndian.Uint32(stszData[off : off+4])
		return uint64(sz), sz > 0
	}

	var chunkCount uint32
	var getChunkOffset func(c uint32) (uint64, bool)
	if len(co64Data) >= 8 {
		chunkCount = binary.BigEndian.Uint32(co64Data[4:8])
		maxPhysicalChunks := uint32((len(co64Data) - 8) / 8)
		if chunkCount == 0 || chunkCount > maxPhysicalChunks {
			return nil, false
		}
		getChunkOffset = func(c uint32) (uint64, bool) {
			if c < 1 || c > chunkCount {
				return 0, false
			}
			off := 8 + int(c-1)*8
			if off+8 > len(co64Data) {
				return 0, false
			}
			return binary.BigEndian.Uint64(co64Data[off : off+8]), true
		}
	} else {
		chunkCount = binary.BigEndian.Uint32(stcoData[4:8])
		maxPhysicalChunks := uint32((len(stcoData) - 8) / 4)
		if chunkCount == 0 || chunkCount > maxPhysicalChunks {
			return nil, false
		}
		getChunkOffset = func(c uint32) (uint64, bool) {
			if c < 1 || c > chunkCount {
				return 0, false
			}
			off := 8 + int(c-1)*4
			if off+4 > len(stcoData) {
				return 0, false
			}
			return uint64(binary.BigEndian.Uint32(stcoData[off : off+4])), true
		}
	}

	stscEntries := binary.BigEndian.Uint32(stscData[4:8])
	maxPhysicalSTSC := uint32((len(stscData) - 8) / 12)
	if stscEntries == 0 || stscEntries > maxPhysicalSTSC {
		return nil, false
	}

	type stscRecord struct {
		firstChunk      uint32
		samplesPerChunk uint32
		descIndex       uint32
	}
	records := make([]stscRecord, stscEntries)
	for i := uint32(0); i < stscEntries; i++ {
		pos := 8 + int(i)*12
		rec := stscRecord{
			firstChunk:      binary.BigEndian.Uint32(stscData[pos : pos+4]),
			samplesPerChunk: binary.BigEndian.Uint32(stscData[pos+4 : pos+8]),
			descIndex:       binary.BigEndian.Uint32(stscData[pos+8 : pos+12]),
		}
		if i == 0 {
			if rec.firstChunk != 1 {
				return nil, false
			}
		} else {
			if rec.firstChunk <= records[i-1].firstChunk {
				return nil, false
			}
		}
		if rec.firstChunk > chunkCount || rec.samplesPerChunk == 0 || rec.descIndex < 1 || rec.descIndex > stsdCount {
			return nil, false
		}
		records[i] = rec
	}

	var sampleRanges []byteRange
	sampleIdx := uint32(1)

	for i := 0; i < len(records); i++ {
		rec := records[i]
		var lastChunk uint32
		if i+1 < len(records) {
			lastChunk = records[i+1].firstChunk - 1
			if lastChunk < rec.firstChunk || lastChunk > chunkCount {
				return nil, false
			}
		} else {
			lastChunk = chunkCount
		}

		for c := rec.firstChunk; c <= lastChunk; c++ {
			if sampleIdx > sampleCount {
				break
			}
			if rec.descIndex == avcDescIndex {
				chunkOff, okOff := getChunkOffset(c)
				sampleSz, okSz := getSampleSize(sampleIdx)
				if okOff && okSz && sampleSz > 0 {
					sampleEnd := chunkOff + sampleSz
					if sampleEnd > chunkOff && sampleEnd <= uint64(MaxVerifiedBytes) {
						sampleRanges = append(sampleRanges, byteRange{
							start: int64(chunkOff),
							end:   int64(sampleEnd),
						})
						if len(sampleRanges) >= maxCandidateSamples {
							return sampleRanges, true
						}
					}
				}
			}
			if uint64(sampleIdx)+uint64(rec.samplesPerChunk) > uint64(math.MaxUint32) {
				return nil, false
			}
			sampleIdx += rec.samplesPerChunk
		}
	}
	return sampleRanges, len(sampleRanges) > 0
}

func findAVCSampleDescIndex(stsd []byte) uint32 {
	if len(stsd) < 8 {
		return 0
	}
	count := binary.BigEndian.Uint32(stsd[4:8])
	entries, ok := boxes(stsd[8:])
	if !ok || count == 0 || int(count) != len(entries) {
		return 0
	}
	for i, entry := range entries {
		if entry.typ != "avc1" && entry.typ != "avc3" {
			continue
		}
		// VisualSampleEntry is 78 bytes; child boxes follow it.
		if len(entry.data) < 78 {
			continue
		}
		children, valid := boxes(entry.data[78:])
		if !valid {
			continue
		}
		for _, child := range children {
			if child.typ == "avcC" && validAVCConfig(child.data) {
				return uint32(i + 1)
			}
		}
	}
	return 0
}

func validAVCConfig(data []byte) bool {
	if len(data) < 7 || data[0] != 1 || data[4]&0xfc != 0xfc || data[5]&0xe0 != 0xe0 {
		return false
	}
	// Require bounded, nonempty SPS and PPS records; metadata only, not decode.
	pos := 6
	spsCount := int(data[5] & 0x1f)
	if spsCount == 0 {
		return false
	}
	for i := 0; i < spsCount; i++ {
		if pos+2 > len(data) {
			return false
		}
		n := int(binary.BigEndian.Uint16(data[pos : pos+2]))
		pos += 2
		if n < 4 || n > 65535 || pos+n > len(data) || data[pos]&0x1f != 7 {
			return false
		}
		pos += n
	}
	if pos >= len(data) {
		return false
	}
	ppsCount := int(data[pos])
	pos++
	if ppsCount == 0 {
		return false
	}
	for i := 0; i < ppsCount; i++ {
		if pos+2 > len(data) {
			return false
		}
		n := int(binary.BigEndian.Uint16(data[pos : pos+2]))
		pos += 2
		if n < 2 || pos+n > len(data) || data[pos]&0x1f != 8 {
			return false
		}
		pos += n
	}
	return true
}
