package telemetry

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"iot-platform/internal/digitaltwin/mapper"
)

type resourceLockTestTx struct {
	pgx.Tx
	queries  []string
	args     [][]any
	failAt   string
	unknown  string
	graphIDs map[string]uuid.UUID
}

func (tx *resourceLockTestTx) QueryRow(_ context.Context, query string, args ...any) pgx.Row {
	tx.queries = append(tx.queries, query)
	tx.args = append(tx.args, args)
	if tx.failAt != "" && strings.Contains(query, tx.failAt) {
		return telemetryFakeRow{err: errors.New("injected query failure")}
	}
	switch {
	case strings.Contains(query, "FROM public.gateways") && strings.Contains(query, "FOR SHARE"):
		if args[0] == tx.unknown {
			return telemetryFakeRow{err: pgx.ErrNoRows}
		}
		return telemetryFakeRow{values: []any{args[0]}}
	case strings.Contains(query, "FROM public.sensors") && strings.Contains(query, "FOR UPDATE"):
		if args[1] == tx.unknown {
			return telemetryFakeRow{err: pgx.ErrNoRows}
		}
		unit := "C"
		return telemetryFakeRow{values: []any{&unit}}
	case strings.Contains(query, "gateway_entity.id"):
		if tx.unknown == "graph" {
			return telemetryFakeRow{err: pgx.ErrNoRows}
		}
		if tx.graphIDs == nil {
			tx.graphIDs = map[string]uuid.UUID{}
		}
		entityID := uuid.NewSHA1(uuid.NameSpaceOID, []byte(fmt.Sprint(args[1])))
		tx.graphIDs[fmt.Sprint(args[1])] = entityID
		return telemetryFakeRow{values: []any{uuid.New(), entityID, true}}
	case strings.Contains(query, "twin_entities") && strings.Contains(query, "FOR UPDATE"):
		return telemetryFakeRow{values: []any{uuid.New()}}
	case strings.Contains(query, "twin_states") && strings.Contains(query, "FOR UPDATE"):
		return telemetryFakeRow{values: []any{uuid.New()}}
	default:
		return telemetryFakeRow{err: errors.New("unexpected resource query")}
	}
}

func resourceTestMessage(gateway, sensor string) *Message {
	return &Message{topic: ParsedTopic{GatewayID: gateway, SensorID: sensor}}
}

func TestPrepareTelemetryResourcesLocksAllPhasesInStableOrder(t *testing.T) {
	tx := &resourceLockTestTx{}
	messages := []*Message{
		resourceTestMessage("gw-b", "sensor-z"),
		resourceTestMessage("gw-a", "sensor-z"),
		resourceTestMessage("gw-a", "sensor-a"),
		resourceTestMessage("gw-b", "sensor-z"), // duplicate resource
	}
	resources, err := prepareTelemetryResources(context.Background(), tx, messages)
	if err != nil {
		t.Fatal(err)
	}
	if len(resources) != 3 {
		t.Fatalf("got %d unique resources, want 3", len(resources))
	}
	var phase int
	var last string
	seen := map[string]bool{}
	for i, query := range tx.queries {
		current := ""
		switch {
		case strings.Contains(query, "FROM public.gateways") && strings.Contains(query, "FOR SHARE"):
			current = "gateway"
		case strings.Contains(query, "FROM public.sensors") && strings.Contains(query, "FOR UPDATE"):
			current = "sensor"
		case strings.Contains(query, "gateway_entity.id"):
			current = "graph"
		case strings.Contains(query, "twin_entities") && strings.Contains(query, "FOR UPDATE"):
			current = "entity"
		case strings.Contains(query, "twin_states") && strings.Contains(query, "FOR UPDATE"):
			current = "state"
		default:
			t.Fatalf("unexpected query at %d: %s", i, query)
		}
		order := map[string]int{"gateway": 0, "sensor": 1, "graph": 2, "entity": 3, "state": 4}
		if order[current] < phase {
			t.Fatalf("phase %s followed later phase %s", current, last)
		}
		if order[current] > phase {
			phase = order[current]
			last = current
		}
		if current == "gateway" || current == "sensor" || current == "entity" || current == "state" {
			key := current + ":" + strings.Join(anyStrings(tx.args[i]), ":")
			if seen[key] {
				t.Fatalf("duplicate lock query %s", key)
			}
			seen[key] = true
		}
	}
	if tx.args[0][0] != "gw-a" || tx.args[1][0] != "gw-b" {
		t.Fatalf("gateway locks not sorted: %#v", tx.args[:2])
	}
	if got := []string{tx.args[2][0].(string) + ":" + tx.args[2][1].(string), tx.args[3][0].(string) + ":" + tx.args[3][1].(string), tx.args[4][0].(string) + ":" + tx.args[4][1].(string)}; strings.Join(got, ",") != "gw-a:sensor-a,gw-a:sensor-z,gw-b:sensor-z" {
		t.Fatalf("sensor locks not sorted: %v", got)
	}
	entityIDs := []string{mapper.SensorEntityID("gw-a", "sensor-a"), mapper.SensorEntityID("gw-a", "sensor-z"), mapper.SensorEntityID("gw-b", "sensor-z")}
	for _, phaseName := range []string{"entity", "state"} {
		var got []string
		for i, query := range tx.queries {
			if phaseName == "entity" && strings.Contains(query, "FROM public.twin_entities") && strings.Contains(query, "FOR UPDATE") || phaseName == "state" && strings.Contains(query, "FROM public.twin_states") && strings.Contains(query, "FOR UPDATE") {
				if phaseName == "entity" {
					got = append(got, tx.args[i][0].(string))
				} else {
					got = append(got, tx.args[i][0].(uuid.UUID).String())
				}
			}
		}
		want := entityIDs
		if phaseName == "state" {
			want = make([]string, len(entityIDs))
			for i, id := range entityIDs {
				want[i] = tx.graphIDs[id].String()
			}
		}
		if strings.Join(got, ",") != strings.Join(want, ",") {
			t.Fatalf("%s locks not in canonical order: %v", phaseName, got)
		}
	}
}

func anyStrings(values []any) []string {
	out := make([]string, len(values))
	for i, value := range values {
		out[i] = fmt.Sprint(value)
	}
	return out
}

func TestPrepareTelemetryResourcesFailures(t *testing.T) {
	for _, test := range []struct {
		name, unknown, failAt string
		want                  ReasonCode
	}{
		{name: "unknown gateway", unknown: "gw-a", want: ReasonUnknownGateway},
		{name: "unknown sensor", unknown: "sensor-a", want: ReasonUnknownSensor},
		{name: "incomplete graph", unknown: "graph", want: ReasonResourceGraphIncomplete},
		{name: "gateway SQL failure", failAt: "FROM public.gateways", want: ReasonDependencyUnavailable},
		{name: "sensor SQL failure", failAt: "FROM public.sensors", want: ReasonDependencyUnavailable},
		{name: "graph SQL failure", failAt: "gateway_entity.id", want: ReasonDependencyUnavailable},
		{name: "entity SQL failure", failAt: "twin_entities", want: ReasonDependencyUnavailable},
		{name: "state SQL failure", failAt: "twin_states", want: ReasonDependencyUnavailable},
	} {
		t.Run(test.name, func(t *testing.T) {
			tx := &resourceLockTestTx{unknown: test.unknown, failAt: test.failAt}
			_, err := prepareTelemetryResources(context.Background(), tx, []*Message{resourceTestMessage("gw-a", "sensor-a")})
			if reasonCode(err) != test.want {
				t.Fatalf("error = %v; reason = %s, want %s", err, reasonCode(err), test.want)
			}
		})
	}
}
