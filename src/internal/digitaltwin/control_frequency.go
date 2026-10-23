package digitaltwin

import (
	"bytes"
	"encoding/json"
	"math/big"
	"regexp"
	"strconv"
	"strings"
)

// Frequency retains exact requested JSON for responses and rational identity
// for hashing. Fields are private so display rounding cannot change intent.
type Frequency struct {
	requested  json.Number
	canonical  string
	intervalUS int64
}

// RequestedHz preserves the exact JSON decimal for a response or stored intent.
func (f Frequency) RequestedHz() json.Number { return f.requested }

// SampleIntervalUS returns the authoritative, half-up rounded integer interval.
func (f Frequency) SampleIntervalUS() int64 { return f.intervalUS }

// EffectiveHz is display-only; intervalUS is the authoritative configuration.
func (f Frequency) EffectiveHz() float64 {
	if f.intervalUS == 0 {
		return 0
	}
	return 1000000 / float64(f.intervalUS)
}

var controlJSONNumber = regexp.MustCompile(`^-?(0|[1-9][0-9]*)(\.[0-9]+)?([eE][+-]?[0-9]+)?$`)

func parseFrequency(raw []byte) (Frequency, error) {
	token := string(bytes.TrimSpace(raw))
	// Length, JSON grammar and exponent magnitude are checked BEFORE SetString.
	// Total token bound also bounds mantissa digits without arbitrary minimum Hz.
	if len(token) > MaxFrequencyTokenBytes || !controlJSONNumber.MatchString(token) {
		return Frequency{}, ErrInvalidFrequency
	}
	if i := strings.IndexAny(token, "eE"); i >= 0 {
		exponent, err := strconv.ParseInt(token[i+1:], 10, 32)
		if err != nil || exponent < -MaxFrequencyExponent || exponent > MaxFrequencyExponent {
			return Frequency{}, ErrInvalidFrequency
		}
	}
	hz, ok := new(big.Rat).SetString(token)
	if !ok || hz.Sign() <= 0 || hz.Cmp(big.NewRat(100, 1)) > 0 {
		return Frequency{}, ErrInvalidFrequency
	}
	period := new(big.Rat).Quo(big.NewRat(1000000, 1), hz)
	q, r := new(big.Int), new(big.Int)
	q.QuoRem(period.Num(), period.Denom(), r)
	if r.Lsh(r, 1).Cmp(period.Denom()) >= 0 {
		q.Add(q, big.NewInt(1))
	}
	if !q.IsInt64() || q.Int64() < MinSampleIntervalUS || q.Int64() > MaxSampleIntervalUS {
		return Frequency{}, ErrFrequencyNotRepresentable
	}
	return Frequency{requested: json.Number(token), canonical: hz.RatString(), intervalUS: q.Int64()}, nil
}
