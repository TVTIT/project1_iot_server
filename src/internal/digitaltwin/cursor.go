package digitaltwin

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"unicode/utf8"
)

// ListCursor represents a decoded and validated pagination cursor for GET /v1/digital-twins.
type ListCursor struct {
	EntityID   string      // k
	GatewayID  *string     // g
	EntityType *EntityType // t
}

// RelationshipCursor represents a decoded and validated pagination cursor for relationships.
type RelationshipCursor struct {
	SourceURN string // s
	RelType   string // r ("hasSensor")
	TargetURN string // k
}

// EncodeListCursor encodes a canonical list cursor string: "v1." + base64url_nopad(JSON).
// It verifies that k is a valid public URN and that filterGatewayID/filterEntityType match k.
func EncodeListCursor(k string, filterGatewayID *string, filterEntityType *EntityType) (string, error) {
	parsedK, err := ParseEntityURN(k)
	if err != nil {
		return "", ErrInvalidCursor
	}

	if filterGatewayID != nil && parsedK.GatewayID != *filterGatewayID {
		return "", ErrInvalidCursor
	}
	if filterEntityType != nil && parsedK.Type != *filterEntityType {
		return "", ErrInvalidCursor
	}

	var gJSON, tJSON string
	if filterGatewayID == nil {
		gJSON = "null"
	} else {
		b, _ := json.Marshal(*filterGatewayID)
		gJSON = string(b)
	}

	if filterEntityType == nil {
		tJSON = "null"
	} else {
		b, _ := json.Marshal(string(*filterEntityType))
		tJSON = string(b)
	}

	kB, _ := json.Marshal(k)
	canonicalJSON := fmt.Sprintf(`{"k":%s,"g":%s,"t":%s}`, string(kB), gJSON, tJSON)
	jsonBytes := []byte(canonicalJSON)

	if len(jsonBytes) > MaxDecodedCursorBytes {
		return "", ErrInvalidCursor
	}

	encoded := CursorVersionPrefix + base64.RawURLEncoding.EncodeToString(jsonBytes)
	if len(encoded) > MaxEncodedCursorLength {
		return "", ErrInvalidCursor
	}

	return encoded, nil
}

// DecodeListCursor decodes, unpacks and verifies list cursor format, keys, and bounds without filter binding.
func DecodeListCursor(cursorStr string) (*ListCursor, error) {
	decoded, err := decodeCursorBytes(cursorStr)
	if err != nil {
		return nil, err
	}

	dec := json.NewDecoder(bytes.NewReader(decoded))
	tok, err := dec.Token()
	if err != nil {
		return nil, ErrInvalidCursor
	}
	delim, ok := tok.(json.Delim)
	if !ok || delim != '{' {
		return nil, ErrInvalidCursor
	}

	type rawVal struct {
		isString bool
		strVal   string
		isNull   bool
	}

	rawMap := make(map[string]rawVal, 3)
	for dec.More() {
		kTok, err := dec.Token()
		if err != nil {
			return nil, ErrInvalidCursor
		}
		key, ok := kTok.(string)
		if !ok {
			return nil, ErrInvalidCursor
		}
		if _, exists := rawMap[key]; exists {
			return nil, ErrInvalidCursor // duplicate key
		}
		if key != "k" && key != "g" && key != "t" {
			return nil, ErrInvalidCursor // unknown key
		}

		vTok, err := dec.Token()
		if err != nil {
			return nil, ErrInvalidCursor
		}
		switch v := vTok.(type) {
		case string:
			if strings.ContainsRune(v, 0) {
				return nil, ErrInvalidCursor
			}
			rawMap[key] = rawVal{isString: true, strVal: v}
		case nil:
			rawMap[key] = rawVal{isNull: true}
		default:
			return nil, ErrInvalidCursor // wrong type
		}
	}

	// Closing }
	tok, err = dec.Token()
	if err != nil {
		return nil, ErrInvalidCursor
	}
	delim, ok = tok.(json.Delim)
	if !ok || delim != '}' {
		return nil, ErrInvalidCursor
	}

	// Must be EOF: no extra document or tokens
	_, err = dec.Token()
	if err != io.EOF {
		return nil, ErrInvalidCursor
	}

	if len(rawMap) != 3 {
		return nil, ErrInvalidCursor // missing key(s)
	}

	kVal, ok := rawMap["k"]
	if !ok || !kVal.isString {
		return nil, ErrInvalidCursor
	}
	parsedK, err := ParseEntityURN(kVal.strVal)
	if err != nil {
		return nil, ErrInvalidCursor
	}

	var gatewayID *string
	gVal := rawMap["g"]
	if gVal.isString {
		gatewayID = &gVal.strVal
	} else if !gVal.isNull {
		return nil, ErrInvalidCursor
	}

	var entityType *EntityType
	tVal := rawMap["t"]
	if tVal.isString {
		et := EntityType(tVal.strVal)
		if !et.Valid() {
			return nil, ErrInvalidCursor
		}
		entityType = &et
	} else if !tVal.isNull {
		return nil, ErrInvalidCursor
	}

	return &ListCursor{
		EntityID:   parsedK.URN,
		GatewayID:  gatewayID,
		EntityType: entityType,
	}, nil
}

// ValidateListCursor validates that decoded list cursor matches the requested filters and k URN.
func ValidateListCursor(c *ListCursor, filterGatewayID *string, filterEntityType *EntityType) error {
	if c == nil {
		return ErrInvalidCursor
	}

	parsedK, err := ParseEntityURN(c.EntityID)
	if err != nil {
		return ErrInvalidCursor
	}

	// Gateway filter binding
	if filterGatewayID != nil {
		if c.GatewayID == nil || *c.GatewayID != *filterGatewayID {
			return ErrInvalidCursor
		}
		if parsedK.GatewayID != *filterGatewayID {
			return ErrInvalidCursor
		}
	} else {
		if c.GatewayID != nil {
			return ErrInvalidCursor
		}
	}

	// EntityType filter binding
	if filterEntityType != nil {
		if c.EntityType == nil || *c.EntityType != *filterEntityType {
			return ErrInvalidCursor
		}
		if parsedK.Type != *filterEntityType {
			return ErrInvalidCursor
		}
	} else {
		if c.EntityType != nil {
			return ErrInvalidCursor
		}
	}

	return nil
}

// ParseListCursor decodes and validates a list cursor against requested filters in one operation.
func ParseListCursor(cursorStr string, filterGatewayID *string, filterEntityType *EntityType) (*ListCursor, error) {
	c, err := DecodeListCursor(cursorStr)
	if err != nil {
		return nil, err
	}
	if err := ValidateListCursor(c, filterGatewayID, filterEntityType); err != nil {
		return nil, err
	}
	return c, nil
}

// EncodeRelationshipCursor encodes a canonical relationship cursor string.
func EncodeRelationshipCursor(sourceURN, relType, targetURN string) (string, error) {
	if relType != "hasSensor" {
		return "", ErrInvalidCursor
	}

	parsedS, err := ParseEntityURN(sourceURN)
	if err != nil || parsedS.Type != EntityTypeGateway {
		return "", ErrInvalidCursor
	}

	parsedT, err := ParseEntityURN(targetURN)
	if err != nil || parsedT.Type != EntityTypeSensor {
		return "", ErrInvalidCursor
	}

	if parsedT.GatewayID != parsedS.GatewayID {
		return "", ErrInvalidCursor
	}

	sB, _ := json.Marshal(sourceURN)
	rB, _ := json.Marshal(relType)
	kB, _ := json.Marshal(targetURN)
	canonicalJSON := fmt.Sprintf(`{"s":%s,"r":%s,"k":%s}`, string(sB), string(rB), string(kB))
	jsonBytes := []byte(canonicalJSON)

	if len(jsonBytes) > MaxDecodedCursorBytes {
		return "", ErrInvalidCursor
	}

	encoded := CursorVersionPrefix + base64.RawURLEncoding.EncodeToString(jsonBytes)
	if len(encoded) > MaxEncodedCursorLength {
		return "", ErrInvalidCursor
	}

	return encoded, nil
}

// DecodeRelationshipCursor decodes and unpacks relationship cursor format, keys, and bounds without source binding.
func DecodeRelationshipCursor(cursorStr string) (*RelationshipCursor, error) {
	decoded, err := decodeCursorBytes(cursorStr)
	if err != nil {
		return nil, err
	}

	dec := json.NewDecoder(bytes.NewReader(decoded))
	tok, err := dec.Token()
	if err != nil {
		return nil, ErrInvalidCursor
	}
	delim, ok := tok.(json.Delim)
	if !ok || delim != '{' {
		return nil, ErrInvalidCursor
	}

	rawMap := make(map[string]string, 3)
	for dec.More() {
		kTok, err := dec.Token()
		if err != nil {
			return nil, ErrInvalidCursor
		}
		key, ok := kTok.(string)
		if !ok {
			return nil, ErrInvalidCursor
		}
		if _, exists := rawMap[key]; exists {
			return nil, ErrInvalidCursor // duplicate key
		}
		if key != "s" && key != "r" && key != "k" {
			return nil, ErrInvalidCursor // unknown key
		}

		vTok, err := dec.Token()
		if err != nil {
			return nil, ErrInvalidCursor
		}
		strVal, ok := vTok.(string)
		if !ok || strings.ContainsRune(strVal, 0) {
			return nil, ErrInvalidCursor // wrong type (e.g. null, number) or contains NUL
		}
		rawMap[key] = strVal
	}

	// Closing }
	tok, err = dec.Token()
	if err != nil {
		return nil, ErrInvalidCursor
	}
	delim, ok = tok.(json.Delim)
	if !ok || delim != '}' {
		return nil, ErrInvalidCursor
	}

	// Must be EOF
	_, err = dec.Token()
	if err != io.EOF {
		return nil, ErrInvalidCursor
	}

	if len(rawMap) != 3 {
		return nil, ErrInvalidCursor // missing key(s)
	}

	sVal := rawMap["s"]
	rVal := rawMap["r"]
	kVal := rawMap["k"]

	parsedS, err := ParseEntityURN(sVal)
	if err != nil || parsedS.Type != EntityTypeGateway {
		return nil, ErrInvalidCursor
	}

	if rVal != "hasSensor" {
		return nil, ErrInvalidCursor
	}

	parsedK, err := ParseEntityURN(kVal)
	if err != nil || parsedK.Type != EntityTypeSensor {
		return nil, ErrInvalidCursor
	}

	if parsedK.GatewayID != parsedS.GatewayID {
		return nil, ErrInvalidCursor
	}

	return &RelationshipCursor{
		SourceURN: parsedS.URN,
		RelType:   rVal,
		TargetURN: parsedK.URN,
	}, nil
}

// ValidateRelationshipCursor validates that relationship cursor source matches expected source entity.
func ValidateRelationshipCursor(c *RelationshipCursor, expectedSourceURN string) error {
	if c == nil {
		return ErrInvalidCursor
	}
	if c.SourceURN != expectedSourceURN {
		return ErrInvalidCursor
	}
	return nil
}

// ParseRelationshipCursor decodes and validates relationship cursor against expected source entity.
func ParseRelationshipCursor(cursorStr string, expectedSourceURN string) (*RelationshipCursor, error) {
	c, err := DecodeRelationshipCursor(cursorStr)
	if err != nil {
		return nil, err
	}
	if err := ValidateRelationshipCursor(c, expectedSourceURN); err != nil {
		return nil, err
	}
	return c, nil
}

func decodeCursorBytes(cursorStr string) ([]byte, error) {
	if len(cursorStr) > MaxEncodedCursorLength {
		return nil, ErrInvalidCursor
	}
	if !strings.HasPrefix(cursorStr, CursorVersionPrefix) {
		return nil, ErrInvalidCursor
	}
	encoded := strings.TrimPrefix(cursorStr, CursorVersionPrefix)
	if encoded == "" || strings.ContainsRune(encoded, '=') {
		return nil, ErrInvalidCursor
	}
	decoded, err := base64.RawURLEncoding.DecodeString(encoded)
	if err != nil {
		return nil, ErrInvalidCursor
	}
	if len(decoded) > MaxDecodedCursorBytes {
		return nil, ErrInvalidCursor
	}
	if !utf8.Valid(decoded) {
		return nil, ErrInvalidCursor
	}
	if bytes.ContainsRune(decoded, 0) {
		return nil, ErrInvalidCursor
	}
	if hasForbiddenJSONEscapes(decoded) {
		return nil, ErrInvalidCursor
	}
	return decoded, nil
}

// hasForbiddenJSONEscapes checks for escaped NUL (\u0000) or UTF-16 surrogate escapes (\uD800..\uDFFF).
func hasForbiddenJSONEscapes(b []byte) bool {
	for i := 0; i+5 < len(b); i++ {
		if b[i] == '\\' && (b[i+1] == 'u' || b[i+1] == 'U') {
			hex4 := string(b[i+2 : i+6])
			// Check NUL escape
			if hex4 == "0000" {
				return true
			}
			// Check surrogate range D800..DFFF
			firstChar := b[i+2]
			if firstChar == 'd' || firstChar == 'D' {
				secondChar := b[i+3]
				if (secondChar >= '8' && secondChar <= '9') ||
					(secondChar >= 'a' && secondChar <= 'f') ||
					(secondChar >= 'A' && secondChar <= 'F') {
					return true
				}
			}
		}
	}
	return false
}
