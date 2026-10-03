//nolint:misspell // Mosquitto executable and process identity must retain their exact spelling.
package mosquittoreload

import (
	"net"
	"os"
	"strings"
	"syscall"

	"golang.org/x/sys/unix"
)

func fileUID(i os.FileInfo) uint32 {
	if st, ok := i.Sys().(*syscall.Stat_t); ok {
		return st.Uid
	}
	return ^uint32(0)
}

func peerUID(c *net.UnixConn) uint32 {
	raw, e := c.SyscallConn()
	if e != nil {
		return ^uint32(0)
	}
	uid := ^uint32(0)
	e = raw.Control(func(fd uintptr) {
		cred, err := unix.GetsockoptUcred(int(fd), unix.SOL_SOCKET, unix.SO_PEERCRED)
		if err == nil {
			uid = cred.Uid
		}
	})
	if e != nil {
		return ^uint32(0)
	}
	return uid
}

type brokerOps struct {
	read  func(string) ([]byte, error)
	link  func(string) (string, error)
	open  func(int, int) (int, error)
	send  func(int, unix.Signal, *unix.Siginfo, int) error
	close func(int) error
}

func signalBroker() error {
	return signalChecked(brokerOps{os.ReadFile, os.Readlink, unix.PidfdOpen, unix.PidfdSendSignal, unix.Close})
}

func brokerIdentity(o brokerOps) (string, error) {
	comm, e := o.read("/proc/1/comm")
	if e != nil || string(comm) != "mosquitto\n" {
		return "", ErrUnavailable
	}
	exe, e := o.link("/proc/1/exe")
	if e != nil || exe != "/usr/sbin/mosquitto" {
		return "", ErrUnavailable
	}
	status, e := o.read("/proc/1/status")
	if e != nil {
		return "", ErrUnavailable
	}
	valid := false
	for _, line := range strings.Split(string(status), "\n") {
		f := strings.Fields(line)
		if len(f) == 5 && f[0] == "Uid:" {
			valid = f[1] == "1883" && f[2] == "1883" && f[3] == "1883" && f[4] == "1883"
		}
	}
	if !valid {
		return "", ErrUnavailable
	}
	stat, e := o.read("/proc/1/stat")
	if e != nil {
		return "", ErrUnavailable
	}
	end := strings.LastIndexByte(string(stat), ')')
	if end < 0 || !strings.HasPrefix(string(stat), "1 (mosquitto)") {
		return "", ErrUnavailable
	}
	fields := strings.Fields(string(stat)[end+1:])
	if len(fields) < 20 {
		return "", ErrUnavailable
	}
	return fields[19], nil // field 22: starttime, process lifetime identity
}

func signalChecked(o brokerOps) error {
	// pidfd pins the intended process. Fail closed on kernels without pidfd;
	// there is deliberately no numeric kill fallback or configurable PID.
	fd, e := o.open(1, 0)
	if e != nil {
		return ErrUnavailable
	}
	defer func() { _ = o.close(fd) }()
	a, e := brokerIdentity(o)
	if e != nil {
		return ErrUnavailable
	}
	b, e := brokerIdentity(o)
	if e != nil || a != b {
		return ErrUnavailable
	}
	if o.send(fd, unix.SIGHUP, nil, 0) != nil {
		return ErrUnavailable
	}
	return nil
}
