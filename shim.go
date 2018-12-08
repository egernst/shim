// Copyright 2017 HyperHQ Inc.
//
// SPDX-License-Identifier: Apache-2.0
//

package main

import (
	"errors"
	"io"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/moby/moby/pkg/term"
	"github.com/sirupsen/logrus"
	context "golang.org/x/net/context"

	pb "github.com/kata-containers/agent/protocols/grpc"
)

const sigChanSize = 2048

var sigIgnored = map[syscall.Signal]bool{
	syscall.SIGCHLD: true,
	syscall.SIGPIPE: true,
}

type shim struct {
	containerID string
	execID      string

	ctx   context.Context
	agent *shimAgent
}

func newShim(ctx context.Context, addr, containerID, execID string) (*shim, error) {
	span, ctx := trace(ctx, "newShim")
	defer span.Finish()

	agent, err := newShimAgent(ctx, addr)
	if err != nil {
		return nil, err
	}

	return &shim{containerID: containerID,
		execID: execID,
		ctx:    ctx,
		agent:  agent}, nil
}

func (s *shim) proxyStdio(wg *sync.WaitGroup, terminal bool) {
	inPipe, outPipe, errPipe := shimStdioPipe(s.ctx, s.agent, s.containerID, s.execID)
	go func() {
		for {
			io.Copy(inPipe, os.Stdin)
		}
		_, err2 := s.agent.CloseStdin(s.ctx, &pb.CloseStdinRequest{
			ContainerId: s.containerID,
			ExecId:      s.execID})
		if err2 != nil {
			logger().WithError(err2).Warn("close stdin failed")
		}
	}()

	go func() {
		for {
			io.Copy(os.Stdout, outPipe)
		}
	}()

	if !terminal {
		go func() {
			for {
				io.Copy(os.Stderr, errPipe)
			}
		}()
	}
}

// handleSignals performs all signal handling.
//
// The tty parameter is specific to SIGWINCH handling.
func (s *shim) handleSignals(ctx context.Context, tty *os.File) chan os.Signal {
	sigc := make(chan os.Signal, sigChanSize)
	// handle all signals for the process.
	signal.Notify(sigc)
	signal.Ignore(syscall.SIGCHLD, syscall.SIGPIPE)

	go func() {
		for sig := range sigc {
			sysSig, ok := sig.(syscall.Signal)
			if !ok {
				err := errors.New("unknown signal")
				logger().WithError(err).WithField("signal", sig.String()).Error()
				continue
			}

			if sigIgnored[sysSig] {
				//ignore these
				continue
			}

			logger().WithField("signal", sig).Debug("handling signal")

			if sysSig == syscall.SIGWINCH {
				s.resizeTty(tty)

				// Don't actually send the signal to the agent
				// in the container since the resize call will
				// request (via the agent) that the kernel send
				// the signal to the real workload.
				continue
			} else if debug && nonFatalSignal(sysSig) {
				// only backtrace in debug mode for security
				// reasons.
				backtrace()
			}

			// forward this signal to container
			_, err := s.agent.SignalProcess(s.ctx, &pb.SignalProcessRequest{
				ContainerId: s.containerID,
				ExecId:      s.execID,
				Signal:      uint32(sysSig)})
			if err != nil {
				logger().WithError(err).WithField("signal", sig.String()).Error("forward signal failed")
			}

			if fatalSignal(sysSig) {
				logger().WithField("signal", sig).Error("received fatal signal")
				die(ctx)
			}
		}
	}()
	return sigc
}

func (s *shim) resizeTty(fromTty *os.File) error {
	fd := fromTty.Fd()

	ws, err := term.GetWinsize(fd)
	if err != nil {
		logger().WithError(err).WithField("fd", fd).Info("Error getting window size")
		return nil
	}

	_, err = s.agent.TtyWinResize(s.ctx, &pb.TtyWinResizeRequest{
		ContainerId: s.containerID,
		ExecId:      s.execID,
		Row:         uint32(ws.Height),
		Column:      uint32(ws.Width)})
	if err != nil {
		logger().WithError(err).WithFields(logrus.Fields{
			"window-height": ws.Height,
			"window-width":  ws.Width,
		}).Error("set window size failed")
	}

	return err
}

func (s *shim) wait() (int32, error) {
	span, _ := trace(s.ctx, "wait")
	defer span.Finish()

	var err error
	var resp *pb.WaitProcessResponse

	for i := 0; i <= 300; i++ {
		resp, err = s.agent.WaitProcess(s.ctx, &pb.WaitProcessRequest{
			ContainerId: s.containerID,
			ExecId:      s.execID})
		if err == nil {
			break
		}

		time.Sleep(100 * time.Millisecond)
	}

	if err != nil {
		return 0, err
	}

	return resp.Status, nil
}
