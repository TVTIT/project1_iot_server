// Fixture-only protected snapshot reader. Never emits native JSON/hash fields.
package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"syscall"
)

func walk(d *json.Decoder, depth int) error {
	if depth > 32 {
		return fmt.Errorf("depth bound")
	}
	t, err := d.Token()
	if err != nil {
		return err
	}
	if delimiter, ok := t.(json.Delim); ok {
		switch delimiter {
		case '{':
			keys := map[string]bool{}
			for d.More() {
				k, err := d.Token()
				if err != nil {
					return err
				}
				key, ok := k.(string)
				if !ok || keys[key] {
					return fmt.Errorf("invalid keys")
				}
				keys[key] = true
				if err := walk(d, depth+1); err != nil {
					return err
				}
			}
		case '[':
			for d.More() {
				if err := walk(d, depth+1); err != nil {
					return err
				}
			}
		default:
			return fmt.Errorf("invalid delimiter")
		}
		_, err = d.Token()
	}
	return err
}

func disabled(raw []byte, target string) (bool, error) {
	if len(raw) > 1<<20 || (target != "A" && target != "B") {
		return false, fmt.Errorf("bound")
	}
	d := json.NewDecoder(bytes.NewReader(raw))
	if err := walk(d, 0); err != nil {
		return false, err
	}
	if _, err := d.Token(); err != io.EOF {
		return false, fmt.Errorf("trailing")
	}
	var store struct {
		Clients []struct {
			Username string `json:"username"`
			Disabled *bool  `json:"disabled"`
		} `json:"clients"`
	}
	if err := json.Unmarshal(raw, &store); err != nil {
		return false, err
	}
	found := false
	value := false
	names := map[string]bool{}
	for _, client := range store.Clients {
		if names[client.Username] {
			return false, fmt.Errorf("duplicate client")
		}
		names[client.Username] = true
		if client.Username == target {
			found = true
			if client.Disabled != nil {
				value = *client.Disabled
			}
		}
	}
	if !found {
		return false, fmt.Errorf("missing target")
	}
	return value, nil
}

func read() (bool, error) {
	if len(os.Args) != 2 {
		return false, fmt.Errorf("arguments")
	}
	fd, err := syscall.Open("/security/dynsec.json", syscall.O_RDONLY|syscall.O_NOFOLLOW, 0)
	if err != nil {
		return false, err
	}
	f := os.NewFile(uintptr(fd), "snapshot")
	defer f.Close()
	before, err := f.Stat()
	if err != nil || !before.Mode().IsRegular() || before.Size() > 1<<20 {
		return false, fmt.Errorf("file")
	}
	raw, err := io.ReadAll(io.LimitReader(f, (1<<20)+1))
	if err != nil {
		return false, err
	}
	after, err := os.Lstat("/security/dynsec.json")
	if err != nil || !os.SameFile(before, after) || before.Size() != after.Size() || !before.ModTime().Equal(after.ModTime()) {
		return false, fmt.Errorf("snapshot raced")
	}
	return disabled(raw, os.Args[1])
}

func main() {
	value, err := read()
	if err != nil {
		fmt.Fprintln(os.Stderr, "snapshot unavailable/invalid")
		os.Exit(1)
	}
	fmt.Println(value)
}
