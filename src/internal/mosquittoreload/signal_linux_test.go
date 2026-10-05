//nolint:misspell // Mosquitto process identity strings must match the actual product.
package mosquittoreload

import (
	"fmt"
	"strings"
	"testing"

	"golang.org/x/sys/unix"
)

func TestSignalIdentity(t *testing.T) {
	for _, kind := range []string{"ok", "open", "comm", "exe", "uid", "stat", "read", "changed", "EPERM", "ESRCH"} {
		t.Run(kind, func(t *testing.T) {
			calls, closed, stats := 0, 0, 0
			o := brokerOps{
				read: func(path string) ([]byte, error) {
					if kind == "read" {
						return nil, fmt.Errorf("secret detail")
					}
					switch path {
					case "/proc/1/comm":
						if kind == "comm" {
							return []byte("init\n"), nil
						}
						return []byte("mosquitto\n"), nil
					case "/proc/1/status":
						if kind == "uid" {
							return []byte("Uid:\t0 0 0 0\n"), nil
						}
						return []byte("Uid:\t1883 1883 1883 1883\n"), nil
					default:
						stats++
						if kind == "stat" {
							return []byte("bad"), nil
						}
						start := 1
						if kind == "changed" {
							start = stats
						}
						return []byte("1 (mosquitto) " + strings.Repeat("0 ", 19) + fmt.Sprint(start)), nil
					}
				},
				link: func(string) (string, error) {
					if kind == "exe" {
						return "/bin/sh", nil
					}
					return "/usr/sbin/mosquitto", nil
				},
				open: func(pid, flags int) (int, error) {
					if pid != 1 || flags != 0 {
						t.Fatal("arbitrary PID")
					}
					if kind == "open" {
						return -1, unix.ENOSYS
					}
					return 42, nil
				},
				send: func(fd int, sig unix.Signal, _ *unix.Siginfo, _ int) error {
					calls++
					if fd != 42 || sig != unix.SIGHUP {
						t.Fatal("wrong signal")
					}
					if kind == "EPERM" {
						return unix.EPERM
					}
					if kind == "ESRCH" {
						return unix.ESRCH
					}
					return nil
				},
				close: func(_ int) error { closed++; return nil },
			}
			e := signalChecked(o)
			if kind == "ok" {
				if e != nil || calls != 1 {
					t.Fatal(e)
				}
			} else if e != ErrUnavailable {
				t.Fatal("unsafe error", e)
			}
			if kind != "open" && closed != 1 {
				t.Fatal("pidfd leaked")
			}
			if kind != "ok" && kind != "EPERM" && kind != "ESRCH" && calls != 0 {
				t.Fatal("wrong process signalled")
			}
		})
	}
}
