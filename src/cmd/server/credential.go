package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net/url"
	"os"
	"time"

	"github.com/eclipse/paho.mqtt.golang/packets"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"iot-platform/internal/config"
	"iot-platform/internal/gateway"
	"iot-platform/internal/httpserver"
	"iot-platform/internal/mqttcredential"
)

type credentialComposition struct {
	manager        httpserver.CredentialManager
	ready          httpserver.CredentialStartupReadiness
	drain          func(context.Context) error
	cleanupTimeout time.Duration
}

func (c credentialComposition) close() error {
	if c.drain == nil {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), c.cleanupTimeout)
	defer cancel()
	return c.drain(ctx)
}

func runCredentialBarrier(ctx context.Context, run, drain func(context.Context) error, timeout time.Duration) error {
	if err := run(ctx); err != nil {
		cleanup, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()
		if drain(cleanup) != nil {
			return fmt.Errorf("credential startup uncertain; controller close unconfirmed")
		}
		return fmt.Errorf("credential startup reconciliation unavailable")
	}
	return nil
}

// One adapter/service owns the serialized maintenance window. No persistent
// manager client or retry worker survives a broker lifetime transition.
func composeCredentials(ctx context.Context, pool *pgxpool.Pool, c config.CredentialConfig) (out credentialComposition, err error) {
	if !c.Enabled {
		return out, nil
	}
	if pool == nil {
		return out, fmt.Errorf("credential database required")
	}
	ctrl := mqttcredential.ControllerClient{ControlDir: c.ControlDir, Timeout: c.ClientTimeout, MaxFrameBytes: 4096}
	out.drain = ctrl.CloseDrain
	out.cleanupTimeout = c.RecoveryTimeout
	defer func() {
		if err != nil {
			_ = out.close()
		}
	}()
	// Close immediately, even if local configuration or repository setup fails.
	if ctrl.CloseDrain(ctx) != nil {
		return out, fmt.Errorf("credential controller unavailable")
	}
	repo, e := mqttcredential.NewPostgresRepositoryWithFinalizationTimeout(pool, c.DBTimeout, c.FinalizeTimeout, c.BatchSize)
	if e != nil {
		return out, e
	}
	st, e := os.Lstat(c.CAFile)
	if e != nil || !st.Mode().IsRegular() || st.Size() > 1<<20 {
		return out, fmt.Errorf("credential CA unavailable")
	}
	ca, e := os.ReadFile(c.CAFile)
	roots := x509.NewCertPool()
	if e != nil || !roots.AppendCertsFromPEM(ca) {
		return out, fmt.Errorf("credential CA invalid")
	}
	u, e := url.Parse(c.ManagementURL)
	if e != nil || u.Hostname() == "" {
		return out, fmt.Errorf("credential management URL invalid")
	}
	protected := []string{mqttcredential.BackendUsername, c.ManagerUsername}
	target := func(u string) bool { return gateway.ValidateGatewayID(u) == nil && u != c.ManagerUsername }
	// An empty DB inventory must not bypass the configured native store check.
	st, e = os.Lstat(c.SnapshotPath)
	if e != nil || !st.Mode().IsRegular() || st.Mode().Perm()&0077 != 0 || st.Mode().Perm()&0400 == 0 || st.Size() > int64(c.MaxSnapshotBytes) {
		return out, fmt.Errorf("credential snapshot unavailable")
	}
	reader, e := mqttcredential.NewDynSecSnapshotReader(mqttcredential.DynSecSnapshotConfig{Path: c.SnapshotPath, WriterUID: 1883, MaxBytes: int64(c.MaxSnapshotBytes), ValidateTarget: target})
	if e != nil {
		return out, e
	}
	newClient := func(ctx context.Context) (*mqttcredential.DynSecClient, error) {
		// RestartClosed confirms a new child lifetime, not listener readiness.
		// Retry fresh connections only within a separate bounded connect budget.
		connectCtx, cancel := context.WithTimeout(ctx, c.ClientTimeout)
		defer cancel()
		for {
			client, err := mqttcredential.NewDynSecClient(connectCtx, mqttcredential.DynSecConfig{BrokerURL: c.ManagementURL, CAPEM: ca, ManagerUsername: c.ManagerUsername, ManagerPassword: c.ManagerPassword, ProtectedUsernames: protected, Timeout: c.ClientTimeout, MaxPayloadBytes: c.MaxPayloadBytes, MaxInflight: c.MaxInflight, QueueSize: c.QueueSize, ValidateTarget: target, ValidateRole: func(u, r string) bool { return target(u) && r == "gateway_"+u }, ManagementDial: ctrl.ManagementDial})
			if err == nil {
				return client, nil
			}
			select {
			case <-connectCtx.Done():
				return nil, mqttcredential.ErrVerificationFailed
			case <-time.After(50 * time.Millisecond):
			}
		}
	}
	login := func(ctx context.Context, user, password string) error {
		return privateCredentialLogin(ctx, ctrl, &tls.Config{RootCAs: roots, ServerName: u.Hostname(), MinVersion: tls.VersionTLS12}, c.ClientTimeout, user, password)
	}
	rejected := func(ctx context.Context, user, password string) error {
		return privateCredentialCheck(ctx, ctrl, &tls.Config{RootCAs: roots, ServerName: u.Hostname(), MinVersion: tls.VersionTLS12}, c.ClientTimeout, user, password, true)
	}
	adapter, e := mqttcredential.NewDynSecAdapter(mqttcredential.DynSecAdapterConfig{Controller: ctrl, NewClient: newClient, Observe: reader.Observe, Login: login, Rejected: rejected, ProtectedUsernames: protected, Timeout: c.OperationTimeout, RecoveryTimeout: c.RecoveryTimeout})
	if e != nil {
		return out, e
	}
	reconciler, e := mqttcredential.NewStartupReconciler(repo, repo, repo, adapter, mqttcredential.StartupOptions{Timeout: c.ReconcileTimeout, PageSize: c.BatchSize, MaxPages: c.MaxPages, VerifyBackend: func(ctx context.Context, r mqttcredential.VerificationReceipt) error {
		d, e := ctrl.Describe(ctx)
		if e != nil || !d.Alive || d.Open || d.Epoch != r.Epoch || d.Nonce != r.Nonce {
			return mqttcredential.ErrLifecycleUnavailable
		}
		if e = login(ctx, mqttcredential.BackendUsername, c.BackendPassword); e != nil {
			return e
		}
		client, e := newClient(ctx)
		if e != nil {
			return e
		}
		client.Close()
		d, e = ctrl.Describe(ctx)
		if e != nil || !d.Alive || d.Open || d.Epoch != r.Epoch || d.Nonce != r.Nonce {
			return mqttcredential.ErrLifecycleUnavailable
		}
		return nil
	}})
	if e != nil {
		return out, e
	}
	service, e := mqttcredential.NewProvisionService(repo, repo, adapter, mqttcredential.ProvisionServiceOptions{RecoveryTimeout: c.RecoveryTimeout, PageSize: c.BatchSize, MaxPages: c.MaxPages, StartupReady: reconciler.Ready})
	if e != nil {
		return out, e
	}
	if e = runCredentialBarrier(ctx, reconciler.Run, ctrl.CloseDrain, c.RecoveryTimeout); e != nil {
		return out, e
	}
	out.manager = service
	out.ready = reconciler
	return out, nil
}

func privateCredentialLogin(ctx context.Context, ctrl mqttcredential.ControllerClient, tlsConfig *tls.Config, timeout time.Duration, user, password string) error {
	return privateCredentialCheck(ctx, ctrl, tlsConfig, timeout, user, password, false)
}

func privateCredentialCheck(ctx context.Context, ctrl mqttcredential.ControllerClient, tlsConfig *tls.Config, timeout time.Duration, user, password string, negative bool) error {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	conn, e := ctrl.DialManagementTLS(ctx, tlsConfig)
	if e != nil {
		return mqttcredential.ErrVerificationFailed
	}
	defer func() { _ = conn.Close() }()
	stop := context.AfterFunc(ctx, func() { _ = conn.Close() })
	defer stop()
	deadline, _ := ctx.Deadline()
	if conn.SetDeadline(deadline) != nil {
		return mqttcredential.ErrVerificationFailed
	}
	id, e := uuid.NewRandom()
	if e != nil {
		return mqttcredential.ErrVerificationFailed
	}
	p := packets.NewControlPacket(packets.Connect).(*packets.ConnectPacket)
	p.ProtocolName = "MQTT"
	p.ProtocolVersion = 4
	p.CleanSession = true
	p.ClientIdentifier = "credential-probe-" + id.String()
	p.UsernameFlag = true
	p.PasswordFlag = true
	p.Username = user
	p.Password = []byte(password)
	defer clear(p.Password)
	if p.Write(conn) != nil {
		return mqttcredential.ErrVerificationFailed
	}
	reply, e := packets.ReadPacket(conn)
	if e != nil {
		return mqttcredential.ErrVerificationFailed
	}
	ack, ok := reply.(*packets.ConnackPacket)
	if !ok || ctx.Err() != nil {
		return mqttcredential.ErrVerificationFailed
	}
	if negative {
		if ack.ReturnCode == packets.ErrRefusedBadUsernameOrPassword || ack.ReturnCode == packets.ErrRefusedNotAuthorised {
			return nil
		}
		return mqttcredential.ErrVerificationFailed
	}
	if ack.ReturnCode != 0 {
		return mqttcredential.ErrVerificationFailed
	}
	return nil
}
