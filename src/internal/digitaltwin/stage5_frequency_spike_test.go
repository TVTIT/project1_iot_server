package digitaltwin

import (
	"encoding/json"
	"errors"
	"math/big"
	"regexp"
	"strconv"
	"strings"
	"testing"
)

// Task 5.0 arithmetic experiment only. No production parser, service or HTTP
// boundary uses this helper; Task 5.1 must implement and test those separately.
func stage5FrequencySpike(number json.Number) (int64, string, error) {
	token := number.String()
	if len(token) > 128 || !stage5SpikeNumber.MatchString(token) {
		return 0, "", errors.New("invalid_frequency")
	}
	if pos := strings.IndexAny(token, "eE"); pos >= 0 {
		exponent, err := strconv.Atoi(token[pos+1:])
		if err != nil || exponent < -128 || exponent > 128 {
			return 0, "", errors.New("invalid_frequency")
		}
	}
	hz, ok := new(big.Rat).SetString(token)
	if !ok || hz.Sign() <= 0 || hz.Cmp(big.NewRat(100, 1)) > 0 {
		return 0, "", errors.New("invalid_frequency")
	}
	period := new(big.Rat).Quo(big.NewRat(1000000, 1), hz)
	quotient, remainder := new(big.Int), new(big.Int)
	quotient.QuoRem(period.Num(), period.Denom(), remainder)
	if remainder.Lsh(remainder, 1).Cmp(period.Denom()) >= 0 {
		quotient.Add(quotient, big.NewInt(1))
	}
	if !quotient.IsInt64() || quotient.Int64() < 10000 || quotient.Int64() > 1<<53 {
		return 0, "", errors.New("frequency_not_representable")
	}
	return quotient.Int64(), hz.RatString(), nil
}

var stage5SpikeNumber = regexp.MustCompile(`^-?(0|[1-9][0-9]*)(\.[0-9]+)?([eE][+-]?[0-9]+)?$`)

func TestStage5FrequencySpike(t *testing.T) {
	for _, test := range []struct {
		hz       string
		interval int64
	}{
		{"100", 10000},
		{"60", 16667},
		{"1.5", 666667},
		{"0.5", 2000000},
		{"0.1", 10000000},
		{"0.01", 100000000},
		{"25.6", 39063}, // exact 39062.5: round half UP, not ties-to-even
		{"0.00000000011102230246251565404236316680908203125", 1 << 53},
	} {
		t.Run(test.hz, func(t *testing.T) {
			interval, canonical, err := stage5FrequencySpike(json.Number(test.hz))
			if err != nil || interval != test.interval || canonical == "" {
				t.Fatalf("Hz %s: interval=%d canonical=%q err=%v; want %d", test.hz, interval, canonical, err, test.interval)
			}
		})
	}
}

func TestStage5FrequencySpikeTinyAndPolicyBounds(t *testing.T) {
	for _, test := range []struct{ hz, code string }{
		{"1e-10", "frequency_not_representable"},
		{"1e-128", "frequency_not_representable"},
		{"0", "invalid_frequency"},
		{"-0.1", "invalid_frequency"},
		{"100.0000000000000000000000000001", "invalid_frequency"},
	} {
		t.Run(test.hz, func(t *testing.T) {
			_, _, err := stage5FrequencySpike(json.Number(test.hz))
			if err == nil || err.Error() != test.code {
				t.Fatalf("Hz %s: err=%v; want %s", test.hz, err, test.code)
			}
		})
	}
}

func TestStage5FrequencySpikeCanonicalDecimal(t *testing.T) {
	for _, hz := range []string{"0.1", "0.10", "1e-1"} {
		interval, canonical, err := stage5FrequencySpike(json.Number(hz))
		if err != nil || interval != 10000000 || canonical != "1/10" {
			t.Fatalf("Hz %s: interval=%d canonical=%q err=%v", hz, interval, canonical, err)
		}
	}
}
