// Isolated fixture ONLY. No automatic broker respawn, no persistent ready file.
package main

import (
	"context"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"regexp"
	"sync/atomic"
	"syscall"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/jackc/pgx/v5"
)

type decision struct {
	Gateway           string
	Revoked           bool
	Epoch             int64
	Operation, Status string
}
type receipt struct {
	Nonce    string
	Verified bool
}

func admit(nonce string, r receipt) bool { return nonce != "" && nonce == r.Nonce && r.Verified }

var operationPattern = regexp.MustCompile(`^[a-f0-9]{32}$`)

func validate(rows []decision) error {
	if len(rows) != 2 {
		return errors.New("inventory cardinality")
	}
	seen := map[string]bool{}
	for _, r := range rows {
		if (r.Gateway != "A" && r.Gateway != "B") || seen[r.Gateway] || r.Epoch < 0 {
			return errors.New("inventory identity")
		}
		seen[r.Gateway] = true
		if r.Revoked && (r.Epoch < 1 || !operationPattern.MatchString(r.Operation) || (r.Status != "pending" && r.Status != "snapshot_observed")) {
			return errors.New("inventory missing revoke job")
		}
		if !r.Revoked && (r.Epoch != 0 || r.Operation != "") {
			return errors.New("inventory inconsistent intent")
		}
	}
	return nil
}
func nonce() string {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		panic("random unavailable")
	}
	return hex.EncodeToString(b)
}
func secret(path string) string {
	b, err := os.ReadFile(path)
	if err != nil || len(b) == 0 || len(b) > 256 {
		panic("fixture credential unavailable")
	}
	return string(b)
}
func wait(t mqtt.Token) error {
	if !t.WaitTimeout(3 * time.Second) {
		return errors.New("MQTT bound")
	}
	return t.Error()
}
func reconcile(n string) (receipt, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	conn, err := pgx.Connect(ctx, "host=db user=postgres dbname=postgres sslmode=disable connect_timeout=2")
	if err != nil {
		return receipt{}, errors.New("DB unavailable")
	}
	defer conn.Close(context.Background())
	tx, err := conn.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.Serializable})
	if err != nil {
		return receipt{}, err
	}
	defer tx.Rollback(context.Background())
	// Fixture writers are serial harness calls. Post-snapshot new revokes are NOT
	// covered by startup receipt, and are pending normal-path work (not built here).
	rows, err := tx.Query(ctx, "SELECT a.gateway,a.revoked,a.epoch,coalesce(j.operation,''),coalesce(j.status,'') FROM authority a LEFT JOIN jobs j USING(gateway) ORDER BY a.gateway")
	if err != nil {
		return receipt{}, err
	}
	var inventory []decision
	for rows.Next() {
		var d decision
		if err = rows.Scan(&d.Gateway, &d.Revoked, &d.Epoch, &d.Operation, &d.Status); err != nil {
			rows.Close()
			return receipt{}, err
		}
		inventory = append(inventory, d)
	}
	rows.Close()
	if err = rows.Err(); err != nil {
		return receipt{}, err
	}
	if err = validate(inventory); err != nil {
		return receipt{}, err
	}
	pool := x509.NewCertPool()
	ca, err := os.ReadFile("/fixture/ca.crt")
	if err != nil || !pool.AppendCertsFromPEM(ca) {
		return receipt{}, errors.New("CA invalid")
	}
	replies := make(chan []byte, 16)
	o := mqtt.NewClientOptions().AddBroker("ssl://127.0.0.1:18884").SetClientID("gate-" + n).SetUsername("manager").SetPassword(secret("/fixture/manager-password")).SetAutoReconnect(false).SetConnectRetry(false).SetConnectTimeout(time.Second).SetTLSConfig(&tls.Config{RootCAs: pool, ServerName: "localhost", MinVersion: tls.VersionTLS12})
	c := mqtt.NewClient(o)
	connected := false
	for i := 0; i < 15; i++ {
		if wait(c.Connect()) == nil {
			connected = true
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if !connected {
		return receipt{}, errors.New("management unavailable")
	}
	defer c.Disconnect(0)
	if err = wait(c.Subscribe("$CONTROL/dynamic-security/v1/response", 0, func(_ mqtt.Client, m mqtt.Message) {
		if len(m.Payload()) <= 65536 {
			select {
			case replies <- append([]byte(nil), m.Payload()...):
			default:
			}
		}
	})); err != nil {
		return receipt{}, err
	}
	request := func(command, gateway string) (json.RawMessage, error) {
		id := nonce()
		payload, _ := json.Marshal(map[string]any{"commands": []any{map[string]any{"command": command, "username": gateway, "correlationData": id}}})
		if e := wait(c.Publish("$CONTROL/dynamic-security/v1", 0, false, payload)); e != nil {
			return nil, e
		}
		timer := time.NewTimer(3 * time.Second)
		defer timer.Stop()
		for count := 0; count < 16; count++ {
			select {
			case raw := <-replies:
				var reply struct {
					Responses []struct {
						Command, CorrelationData, Error string
						Data                            json.RawMessage
					}
				}
				if json.Unmarshal(raw, &reply) != nil {
					return nil, errors.New("reply invalid")
				}
				for _, r := range reply.Responses {
					if r.CorrelationData == id {
						if r.Command != command || r.Error != "" {
							return nil, errors.New("reply mismatch")
						}
						return r.Data, nil
					}
				}
			case <-timer.C:
				return nil, errors.New("reply bound")
			case <-ctx.Done():
				return nil, errors.New("reconcile bound")
			}
		}
		return nil, errors.New("reply flood")
	}
	for _, d := range inventory {
		if !d.Revoked {
			continue
		}
		if _, err = request("disableClient", d.Gateway); err != nil {
			return receipt{}, err
		}
		raw, e := request("getClient", d.Gateway)
		if e != nil {
			return receipt{}, e
		}
		var state struct{ Client struct{ Disabled bool } }
		if json.Unmarshal(raw, &state) != nil || !state.Client.Disabled {
			return receipt{}, errors.New("RAM not disabled")
		}
		fmt.Println("QUERY_VERIFIED", n)
		if os.Getenv("GATE_FAULT") == "after-query" {
			time.Sleep(10 * time.Second)
			return receipt{}, errors.New("fault after query")
		}
		cmd := exec.CommandContext(ctx, "/snapshot", d.Gateway)
		out, e := cmd.Output()
		if e != nil || string(out) != "true\n" {
			return receipt{}, errors.New("snapshot not disabled")
		}
		if _, err = tx.Exec(ctx, "UPDATE jobs SET status='snapshot_observed',attempts=attempts+1 WHERE operation=$1", d.Operation); err != nil {
			return receipt{}, err
		}
	}
	if err = tx.Commit(ctx); err != nil {
		return receipt{}, err
	}
	return receipt{Nonce: n, Verified: true}, nil
}
func proxy(listener net.Listener, target string) {
	sem := make(chan struct{}, 32)
	for {
		incoming, e := listener.Accept()
		if e != nil {
			return
		}
		select {
		case sem <- struct{}{}:
		default:
			incoming.Close()
			continue
		}
		go func() {
			defer func() { <-sem }()
			defer incoming.Close()
			upstream, e := net.DialTimeout("tcp", target, time.Second)
			if e != nil {
				return
			}
			defer upstream.Close()
			incoming.SetDeadline(time.Now().Add(15 * time.Second))
			upstream.SetDeadline(time.Now().Add(15 * time.Second))
			done := make(chan struct{}, 1)
			go func() { io.Copy(upstream, incoming); done <- struct{}{} }()
			io.Copy(incoming, upstream)
			upstream.Close()
			incoming.Close()
			<-done
		}()
	}
}
func main() {
	// PID1 wrapper owns child and sockets. Child may die while proxy still bound,
	// but cannot be replaced: no loop, restart policy, external supervisor or path
	// to a new child. Wrapper exit destroys all sockets; next lifetime starts closed.
	n := nonce()
	fmt.Println("CLOSED", n)
	os.Setenv("PGPASSWORD", secret("/fixture/db-password"))
	child := exec.Command("mosquitto", "-c", "/fixture/broker.conf")
	child.Stdout = os.Stdout
	child.Stderr = os.Stderr
	child.SysProcAttr = &syscall.SysProcAttr{Pdeathsig: syscall.SIGKILL}
	if child.Start() != nil {
		fmt.Println("BLOCKED child")
		os.Exit(1)
	}
	var dead atomic.Bool
	go func() { child.Wait(); dead.Store(true); fmt.Println("CLOSED child-death", n); os.Exit(1) }()
	r, err := reconcile(n)
	if err != nil || !admit(n, r) || dead.Load() {
		fmt.Println("BLOCKED reconcile", n)
		child.Process.Kill()
		os.Exit(1)
	}
	// Both host-mapped TLS and the tunnel-route simulation terminate here, NEVER
	// at management. Receipts exist solely on this stack; no file can open ingress.
	l, e := net.Listen("tcp", ":8883")
	if e != nil {
		child.Process.Kill()
		os.Exit(1)
	}
	t, e := net.Listen("tcp", ":8884")
	if e != nil {
		l.Close()
		child.Process.Kill()
		os.Exit(1)
	}
	fmt.Println("OPEN", n)
	go proxy(t, "127.0.0.1:18884")
	proxy(l, "127.0.0.1:18884")
}
