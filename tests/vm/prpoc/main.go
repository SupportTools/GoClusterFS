// Command prpoc runs single fencing steps on a real device, for the B key-check POC: register, reserve, list,
// preempt, write, and a re-register through fence.Rekeyer with the heartbeat age frozen at 1s and the slot live, as
// after a hypervisor pause the peer's preempt beat its Dead mark.
package main

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"syscall"
	"time"
	"unsafe"

	"github.com/supporttools/GoClusterFS/internal/fence"
)

func main() {
	if len(os.Args) < 4 {
		fmt.Fprintln(os.Stderr, "usage: prpoc DEVICE KEY register|reserve|unregister|clear|list|write|preempt VICTIM|rekey [-nocheck] [-wait FILE]")
		os.Exit(2)
	}
	dev, cmd := os.Args[1], os.Args[3]
	key, err := strconv.ParseUint(os.Args[2], 0, 64)
	if err != nil {
		fail(err)
	}
	f, err := os.OpenFile(dev, os.O_RDWR, 0)
	if err != nil {
		fail(err)
	}
	pr := fence.Block(f, key)
	switch cmd {
	case "register":
		report(pr.Register())
	case "reserve":
		report(pr.Reserve())
	case "unregister":
		report(pr.Unregister())
	case "clear":
		report(pr.Clear())
	case "list":
		gen, n, err := pr.Registrations()
		keys, _ := pr.Keys()
		paths, _ := pr.Paths()
		fmt.Printf("generation=%d registrations_of_key=%d paths=%d keys=%x err=%v\n", gen, n, paths, keys, err)
	case "preempt":
		victim, err := strconv.ParseUint(os.Args[4], 0, 64)
		if err != nil {
			fail(err)
		}
		report(pr.Preempt(victim))
	case "write":
		report(write(dev))
	case "rekey":
		rekey(pr, os.Args[4:])
	default:
		fail(fmt.Errorf("unknown command %q", cmd))
	}
}

func rekey(pr *fence.PR, args []string) {
	k := &fence.Rekeyer{
		PR:           pr,
		HoldWrites:   func(fn func() error) error { return fn() },
		HeartbeatAge: func() time.Duration { return time.Second },
		SlotLive:     func() error { return nil },
		StallLimit:   15 * time.Second,
		Listed:       func(l fence.KeyListing) { fmt.Printf("listing %+v\n", l) },
	}
	for i := 0; i < len(args); i++ {
		switch args[i] {
		case "-nocheck":
			k.NoKeyCheck = true
		case "-wait":
			i++
			file := args[i]
			k.BeforeRegister = func() {
				fmt.Println("before-register")
				for {
					if _, err := os.Stat(file); err == nil {
						return
					}
					time.Sleep(50 * time.Millisecond)
				}
			}
		}
	}
	report(k.Rekey())
}

// write writes 4 KiB with O_DIRECT at 256 MiB.
func write(dev string) error {
	f, err := os.OpenFile(dev, os.O_RDWR|syscall.O_DIRECT, 0)
	if err != nil {
		return err
	}
	defer f.Close()
	buf := make([]byte, 8192)
	off := int(uintptr(unsafe.Pointer(&buf[0])) % 4096)
	if off != 0 {
		off = 4096 - off
	}
	_, err = f.WriteAt(buf[off:off+4096], 256<<20)
	if errors.Is(err, syscall.EBADE) {
		return fmt.Errorf("refused by the reservation (EBADE)")
	}
	return err
}

func report(err error) {
	if err != nil {
		fmt.Printf("result: %v\n", err)
		return
	}
	fmt.Println("result: ok")
}

func fail(err error) {
	fmt.Fprintln(os.Stderr, err)
	os.Exit(1)
}
