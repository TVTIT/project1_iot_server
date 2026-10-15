package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"net"
	"net/url"
	"time"

	"github.com/eclipse/paho.mqtt.golang/packets"
	"github.com/google/uuid"

	"iot-platform/internal/mqttcredential"
)

type probeDialerFunc func(ctx context.Context, network, addr string) (net.Conn, error)

func buildVerifiedTLSConfig(caPEM []byte, mgmtURL string) (*tls.Config, string, error) {
	if len(caPEM) == 0 {
		return nil, "", errors.New("credential CA invalid")
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(caPEM) {
		return nil, "", errors.New("credential CA invalid")
	}

	u, err := url.Parse(mgmtURL)
	if err != nil || u == nil {
		return nil, "", errors.New("credential management URL invalid")
	}
	switch u.Scheme {
	case "ssl", "tls", "mqtts":
	default:
		return nil, "", errors.New("credential management URL invalid")
	}

	hostname := u.Hostname()
	port := u.Port()
	if hostname == "" || port == "" {
		return nil, "", errors.New("credential management URL invalid")
	}

	target := net.JoinHostPort(hostname, port)
	cfg := &tls.Config{
		RootCAs:            roots,
		ServerName:         hostname,
		MinVersion:         tls.VersionTLS12,
		InsecureSkipVerify: false,
	}

	return cfg, target, nil
}

func probePrivateCredential(ctx context.Context, tlsConfig *tls.Config, target string, timeout time.Duration, user, password string, negative bool) error {
	return probePrivateCredentialWithDialer(ctx, nil, tlsConfig, target, timeout, user, password, negative)
}

func probePrivateCredentialWithDialer(ctx context.Context, dial probeDialerFunc, tlsConfig *tls.Config, target string, timeout time.Duration, user, password string, negative bool) error {
	if timeout <= 0 || tlsConfig == nil || target == "" || tlsConfig.InsecureSkipVerify {
		return mqttcredential.ErrVerificationFailed
	}

	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	var conn net.Conn
	var err error
	if dial != nil {
		conn, err = dial(ctx, "tcp", target)
	} else {
		d := &tls.Dialer{
			Config: tlsConfig,
		}
		conn, err = d.DialContext(ctx, "tcp", target)
	}
	if err != nil {
		return mqttcredential.ErrVerificationFailed
	}
	defer func() { _ = conn.Close() }()

	stop := context.AfterFunc(ctx, func() { _ = conn.Close() })
	defer stop()

	deadline, _ := ctx.Deadline()
	if conn.SetDeadline(deadline) != nil {
		return mqttcredential.ErrVerificationFailed
	}

	id, err := uuid.NewRandom()
	if err != nil {
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

	reply, err := packets.ReadPacket(conn)
	if err != nil {
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
