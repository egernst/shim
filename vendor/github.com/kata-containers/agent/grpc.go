//
// Copyright (c) 2017 Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0
//

package main

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"regexp"
	"syscall"
	"time"

	gpb "github.com/gogo/protobuf/types"
	pb "github.com/kata-containers/agent/protocols/grpc"
	"github.com/opencontainers/runc/libcontainer"
	"github.com/opencontainers/runc/libcontainer/configs"
	"github.com/opencontainers/runc/libcontainer/specconv"
	"github.com/opencontainers/runc/libcontainer/utils"
	"github.com/opencontainers/runtime-spec/specs-go"
	"github.com/sirupsen/logrus"
	"golang.org/x/net/context"
	"golang.org/x/sys/unix"
)

type agentGRPC struct {
	sandbox *sandbox
	version string
}

// PCI scanning
const (
	pciBusRescanFile = "/sys/bus/pci/rescan"
	pciBusMode       = 0220
)

// CPU and Memory hotplug
const (
	sysfsCPUOnlinePath = "/sys/devices/system/cpu"
	sysfsMemOnlinePath = "/sys/devices/system/memory"
	cpuRegexpPattern   = "cpu[0-9]*"
	memRegexpPattern   = "memory[0-9]*"
)

type onlineResource struct {
	sysfsOnlinePath string
	regexpPattern   string
}

var defaultCapsList = []string{
	"CAP_CHOWN",
	"CAP_DAC_OVERRIDE",
	"CAP_FOWNER",
	"CAP_FSETID",
	"CAP_KILL",
	"CAP_SETGID",
	"CAP_SETUID",
	"CAP_SETPCAP",
	"CAP_NET_BIND_SERVICE",
	"CAP_NET_RAW",
	"CAP_SYS_CHROOT",
	"CAP_MKNOD",
	"CAP_AUDIT_WRITE",
	"CAP_SETFCAP",
}

var fullCapsList = []string{
	"CAP_AUDIT_CONTROL",
	"CAP_AUDIT_READ",
	"CAP_AUDIT_WRITE",
	"CAP_BLOCK_SUSPEND",
	"CAP_CHOWN",
	"CAP_DAC_OVERRIDE",
	"CAP_DAC_READ_SEARCH",
	"CAP_FOWNER",
	"CAP_FSETID",
	"CAP_IPC_LOCK",
	"CAP_IPC_OWNER",
	"CAP_KILL",
	"CAP_LEASE",
	"CAP_LINUX_IMMUTABLE",
	"CAP_MAC_ADMIN",
	"CAP_MAC_OVERRIDE",
	"CAP_MKNOD",
	"CAP_NET_ADMIN",
	"CAP_NET_BIND_SERVICE",
	"CAP_NET_BROADCAST",
	"CAP_NET_RAW",
	"CAP_SETGID",
	"CAP_SETFCAP",
	"CAP_SETPCAP",
	"CAP_SETUID",
	"CAP_SYS_ADMIN",
	"CAP_SYS_BOOT",
	"CAP_SYS_CHROOT",
	"CAP_SYS_MODULE",
	"CAP_SYS_NICE",
	"CAP_SYS_PACCT",
	"CAP_SYS_PTRACE",
	"CAP_SYS_RAWIO",
	"CAP_SYS_RESOURCE",
	"CAP_SYS_TIME",
	"CAP_SYS_TTY_CONFIG",
	"CAP_SYSLOG",
	"CAP_WAKE_ALARM",
}

var defaultMountFlags = unix.MS_NOEXEC | unix.MS_NOSUID | unix.MS_NODEV

var defaultMounts = []*configs.Mount{
	{
		Source:      "proc",
		Destination: "/proc",
		Device:      "proc",
		Flags:       defaultMountFlags,
	},
	{
		Source:      "tmpfs",
		Destination: "/dev",
		Device:      "tmpfs",
		Flags:       syscall.MS_NOSUID | syscall.MS_STRICTATIME,
		Data:        "mode=755",
	},
	{
		Source:      "devpts",
		Destination: "/dev/pts",
		Device:      "devpts",
		Flags:       syscall.MS_NOSUID | syscall.MS_NOEXEC,
		Data:        "newinstance,ptmxmode=0666,mode=0620,gid=5",
	},
	{
		Device:      "tmpfs",
		Source:      "shm",
		Destination: "/dev/shm",
		Data:        "mode=1777,size=65536k",
		Flags:       defaultMountFlags,
	},
	{
		Source:      "mqueue",
		Destination: "/dev/mqueue",
		Device:      "mqueue",
		Flags:       defaultMountFlags,
	},
	{
		Source:      "sysfs",
		Destination: "/sys",
		Device:      "sysfs",
		Flags:       defaultMountFlags,
	},
}

var emptyResp = &gpb.Empty{}

func onlineCPUMem() error {
	resourceList := []onlineResource{
		{
			sysfsOnlinePath: sysfsCPUOnlinePath,
			regexpPattern:   cpuRegexpPattern,
		},
		{
			sysfsOnlinePath: sysfsMemOnlinePath,
			regexpPattern:   memRegexpPattern,
		},
	}

	for _, resource := range resourceList {
		files, err := ioutil.ReadDir(resource.sysfsOnlinePath)
		if err != nil {
			return err
		}

		for _, file := range files {
			matched, err := regexp.MatchString(resource.regexpPattern, file.Name())
			if err != nil {
				return err
			}

			if !matched {
				continue
			}

			cpuOnlinePath := filepath.Join(sysfsCPUOnlinePath, file.Name(), "online")
			ioutil.WriteFile(cpuOnlinePath, []byte("1"), 0600)
		}
	}

	return nil
}

func setConsoleCarriageReturn(fd int) error {
	termios, err := unix.IoctlGetTermios(fd, unix.TCGETS)
	if err != nil {
		return err
	}

	termios.Oflag |= unix.ONLCR

	return unix.IoctlSetTermios(fd, unix.TCSETS, termios)
}

func buildProcess(agentProcess *pb.Process, procID string) (*process, error) {
	user := agentProcess.User.Username
	if user == "" {
		// We can specify the user and the group separated by ":"
		user = fmt.Sprintf("%d:%d", agentProcess.User.UID, agentProcess.User.GID)
	}

	additionalGids := []string{}
	for _, gid := range agentProcess.User.AdditionalGids {
		additionalGids = append(additionalGids, fmt.Sprintf("%d", gid))
	}

	proc := &process{
		id: procID,
		process: libcontainer.Process{
			Cwd:              agentProcess.Cwd,
			Args:             agentProcess.Args,
			Env:              agentProcess.Env,
			User:             user,
			AdditionalGroups: additionalGids,
		},
	}

	if agentProcess.Terminal {
		parentSock, childSock, err := utils.NewSockPair("console")
		if err != nil {
			return nil, err
		}

		proc.process.ConsoleSocket = childSock
		proc.consoleSock = parentSock

		return proc, nil
	}

	rStdin, wStdin, err := os.Pipe()
	if err != nil {
		return nil, err
	}

	rStdout, wStdout, err := os.Pipe()
	if err != nil {
		return nil, err
	}

	rStderr, wStderr, err := os.Pipe()
	if err != nil {
		return nil, err
	}

	proc.process.Stdin = rStdin
	proc.process.Stdout = wStdout
	proc.process.Stderr = wStderr

	proc.stdin = wStdin
	proc.stdout = rStdout
	proc.stderr = rStderr

	return proc, nil
}

func (a *agentGRPC) Check(ctx context.Context, req *pb.CheckRequest) (*pb.HealthCheckResponse, error) {
	return &pb.HealthCheckResponse{Status: pb.HealthCheckResponse_SERVING}, nil
}

func (a *agentGRPC) Version(ctx context.Context, req *pb.CheckRequest) (*pb.VersionCheckResponse, error) {
	return &pb.VersionCheckResponse{
		GrpcVersion:  pb.APIVersion,
		AgentVersion: a.version,
	}, nil

}

// Shared function between StartContainer and ExecProcess, because those expect
// a process to be run. The difference being the process does not exist yet in
// case of ExecProcess.
func (a *agentGRPC) runProcess(cid, execID string, agentProcess *pb.Process) (err error) {
	if a.sandbox.running == false {
		return fmt.Errorf("Sandbox not started")
	}

	ctr, err := a.sandbox.getContainer(cid)
	if err != nil {
		return err
	}

	status, err := ctr.container.Status()
	if err != nil {
		return err
	}

	var proc *process
	if agentProcess != nil && execID != "" {
		if status != libcontainer.Running {
			return fmt.Errorf("Container %s status %s, should be %s", cid, status.String(), libcontainer.Running.String())
		}

		proc, err = buildProcess(agentProcess, execID)
		if err != nil {
			return err
		}
	} else {
		if status != libcontainer.Created {
			return fmt.Errorf("Container %s status %s, should be %s", cid, status.String(), libcontainer.Created.String())
		}

		proc = ctr.initProcess
	}

	// This lock is very important to avoid any race with reaper.reap().
	// Indeed, if we don't lock this here, we could potentially get the
	// SIGCHLD signal before the channel has been created, meaning we will
	// miss the opportunity to get the exit code, leading WaitProcess() to
	// wait forever on the new channel.
	// This lock has to be taken before we run the new process.
	a.sandbox.subreaper.RLock()
	defer a.sandbox.subreaper.RUnlock()

	if err := ctr.container.Run(&(proc.process)); err != nil {
		return fmt.Errorf("Could not run process: %v", err)
	}
	defer proc.closePostStartFDs()

	// Get process PID
	pid, err := proc.process.Pid()
	if err != nil {
		return err
	}

	proc.exitCodeCh = make(chan int, 1)

	// Create process channel to allow WaitProcess to wait on it.
	// This channel is buffered so that reaper.reap() will not
	// block until WaitProcess listen onto this channel.
	a.sandbox.subreaper.setExitCodeCh(pid, proc.exitCodeCh)

	// Setup terminal if enabled.
	if proc.consoleSock != nil {
		termMaster, err := utils.RecvFd(proc.consoleSock)
		if err != nil {
			return err
		}

		if err := setConsoleCarriageReturn(int(termMaster.Fd())); err != nil {
			return err
		}

		proc.termMaster = termMaster
	}

	// Save process info.
	ctr.setProcess(execID, proc)

	return nil
}

// This function updates the container namespaces configuration based on the
// sandbox information. When the sandbox is created, it can be setup in a way
// that all containers will share some specific namespaces. This is the agent
// responsibility to create those namespaces so that they can be shared across
// several containers.
// If the sandbox has not been setup to share namespaces, then we assume all
// containers will be started in their own new namespace.
// The value of a.sandbox.sharedPidNs.path will always override the namespace
// path set by the spec, since we will always ignore it. Indeed, it makes no
// sense to rely on the namespace path provided by the host since namespaces
// are different inside the guest.
func (a *agentGRPC) updateContainerConfigNamespaces(config *configs.Config) error {
	// Update shared PID namespace.
	for idx, ns := range config.Namespaces {
		if ns.Type == configs.NEWPID {
			// In case the path is empty because we don't expect
			// the containers to share the same PID namespace, a
			// new PID ns is going to be created.
			config.Namespaces[idx].Path = a.sandbox.sharedPidNs.path
			return nil
		}
	}

	// If no NEWPID type was found, let's make sure we add it. Otherwise,
	// the container could end up in the same PID namespace than the agent
	// and we want to prevent this for security reasons.
	newPidNs := configs.Namespace{
		Type: configs.NEWPID,
		Path: a.sandbox.sharedPidNs.path,
	}

	config.Namespaces = append(config.Namespaces, newPidNs)

	return nil
}

func (a *agentGRPC) updateContainerConfigPrivileges(spec *specs.Spec, config *configs.Config) error {
	if spec == nil || spec.Process == nil {
		// Don't throw an error in case the Spec does not contain any
		// information about NoNewPrivileges.
		return nil
	}

	// Add the value for NoNewPrivileges option.
	config.NoNewPrivileges = spec.Process.NoNewPrivileges

	return nil
}

func (a *agentGRPC) updateContainerConfig(spec *specs.Spec, config *configs.Config) error {
	if err := a.updateContainerConfigNamespaces(config); err != nil {
		return err
	}

	return a.updateContainerConfigPrivileges(spec, config)
}

func (a *agentGRPC) CreateContainer(ctx context.Context, req *pb.CreateContainerRequest) (*gpb.Empty, error) {
	if a.sandbox.running == false {
		return emptyResp, fmt.Errorf("Sandbox not started, impossible to run a new container")
	}

	if _, err := a.sandbox.getContainer(req.ContainerId); err == nil {
		return emptyResp, fmt.Errorf("Container %s already exists, impossible to create", req.ContainerId)
	}

	// re-scan PCI bus
	// looking for hidden devices
	if err := ioutil.WriteFile(pciBusRescanFile, []byte("1"), pciBusMode); err != nil {
		agentLog.WithError(err).Warn("Could not rescan PCI bus")
	}

	mountList, err := addMounts(req.Storages)
	if err != nil {
		return emptyResp, err
	}

	// Convert the spec to an actual OCI specification structure.
	ociSpec, err := pb.GRPCtoOCI(req.OCI)
	if err != nil {
		return emptyResp, err
	}

	// Convert the OCI specification into a libcontainer configuration.
	config, err := specconv.CreateLibcontainerConfig(&specconv.CreateOpts{
		CgroupName:   req.ContainerId,
		NoNewKeyring: true,
		Spec:         ociSpec,
	})
	if err != nil {
		return emptyResp, err
	}

	// Update libcontainer configuration for specific cases not handled
	// by the specconv converter.
	if err := a.updateContainerConfig(ociSpec, config); err != nil {
		return emptyResp, err
	}

	containerPath := filepath.Join("/tmp/libcontainer", a.sandbox.id)
	factory, err := libcontainer.New(containerPath, libcontainer.Cgroupfs)
	if err != nil {
		return emptyResp, err
	}

	libContContainer, err := factory.Create(req.ContainerId, config)
	if err != nil {
		return emptyResp, err
	}

	builtProcess, err := buildProcess(req.OCI.Process, req.ExecId)
	if err != nil {
		return emptyResp, err
	}

	container := &container{
		id:          req.ContainerId,
		initProcess: builtProcess,
		container:   libContContainer,
		config:      *config,
		processes:   make(map[string]*process),
		mounts:      mountList,
	}

	a.sandbox.setContainer(req.ContainerId, container)

	return emptyResp, nil
}

func (a *agentGRPC) StartContainer(ctx context.Context, req *pb.StartContainerRequest) (*gpb.Empty, error) {
	if err := a.runProcess(req.ContainerId, "", nil); err != nil {
		return emptyResp, err
	}

	return emptyResp, nil
}

func (a *agentGRPC) ExecProcess(ctx context.Context, req *pb.ExecProcessRequest) (*gpb.Empty, error) {
	if err := a.runProcess(req.ContainerId, req.ExecId, req.Process); err != nil {
		return emptyResp, err
	}

	return emptyResp, nil
}

func (a *agentGRPC) SignalProcess(ctx context.Context, req *pb.SignalProcessRequest) (*gpb.Empty, error) {
	if a.sandbox.running == false {
		return emptyResp, fmt.Errorf("Sandbox not started, impossible to signal the container")
	}

	ctr, err := a.sandbox.getContainer(req.ContainerId)
	if err != nil {
		return emptyResp, fmt.Errorf("Could not signal process %s: %v", req.ExecId, err)
	}

	status, err := ctr.container.Status()
	if err != nil {
		return emptyResp, err
	}

	signal := syscall.Signal(req.Signal)

	if status == libcontainer.Stopped {
		agentLog.WithFields(logrus.Fields{
			"containerID": req.ContainerId,
			"sandbox":     a.sandbox.id,
			"signal":      signal.String(),
		}).Info("discarding signal as container stopped")
	}

	// If the exec ID provided is empty, let's apply the signal to all
	// processes inside the container.
	// If the process is the container process, let's use the container
	// API for that.
	if req.ExecId == "" {
		return emptyResp, ctr.container.Signal(signal, true)
	} else if ctr.initProcess.id == req.ExecId {
		return emptyResp, ctr.container.Signal(signal, false)
	}

	proc, err := ctr.getProcess(req.ExecId)
	if err != nil {
		return emptyResp, fmt.Errorf("Could not signal process: %v", err)
	}

	if err := proc.process.Signal(signal); err != nil {
		return emptyResp, err
	}

	return emptyResp, nil
}

func (a *agentGRPC) WaitProcess(ctx context.Context, req *pb.WaitProcessRequest) (*pb.WaitProcessResponse, error) {
	proc, ctr, err := a.sandbox.getRunningProcess(req.ContainerId, req.ExecId)
	if err != nil {
		return &pb.WaitProcessResponse{}, err
	}

	defer func() {
		proc.closePostExitFDs()
		ctr.deleteProcess(proc.id)
	}()

	// Using helper function wait() to deal with the subreaper.
	libContProcess := (*reaperLibcontainerProcess)(&(proc.process))
	exitCode, err := a.sandbox.subreaper.wait(proc.exitCodeCh, libContProcess)
	if err != nil {
		return &pb.WaitProcessResponse{}, err
	}

	return &pb.WaitProcessResponse{
		Status: int32(exitCode),
	}, nil
}

func (a *agentGRPC) RemoveContainer(ctx context.Context, req *pb.RemoveContainerRequest) (*gpb.Empty, error) {
	ctr, err := a.sandbox.getContainer(req.ContainerId)
	if err != nil {
		return emptyResp, err
	}

	timeout := int(req.Timeout)

	a.sandbox.Lock()
	defer a.sandbox.Unlock()

	if timeout == 0 {
		if err := ctr.removeContainer(); err != nil {
			return emptyResp, err
		}
	} else {
		done := make(chan error)
		go func() {
			if err := ctr.removeContainer(); err != nil {
				done <- err
			}

			close(done)
		}()

		select {
		case err := <-done:
			if err != nil {
				return emptyResp, err
			}
		case <-time.After(time.Duration(req.Timeout) * time.Second):
			return emptyResp, fmt.Errorf("Timeout reached after %ds", timeout)
		}
	}

	delete(a.sandbox.containers, ctr.id)

	return emptyResp, nil
}

func (a *agentGRPC) WriteStdin(ctx context.Context, req *pb.WriteStreamRequest) (*pb.WriteStreamResponse, error) {
	proc, _, err := a.sandbox.getRunningProcess(req.ContainerId, req.ExecId)
	if err != nil {
		return &pb.WriteStreamResponse{}, err
	}

	var file *os.File
	if proc.termMaster != nil {
		file = proc.termMaster
	} else {
		file = proc.stdin
	}

	n, err := file.Write(req.Data)
	if err != nil {
		return &pb.WriteStreamResponse{}, err
	}

	return &pb.WriteStreamResponse{
		Len: uint32(n),
	}, nil
}

func (a *agentGRPC) ReadStdout(ctx context.Context, req *pb.ReadStreamRequest) (*pb.ReadStreamResponse, error) {
	data, err := a.sandbox.readStdio(req.ContainerId, req.ExecId, int(req.Len), true)
	if err != nil {
		return &pb.ReadStreamResponse{}, err
	}

	return &pb.ReadStreamResponse{
		Data: data,
	}, nil
}

func (a *agentGRPC) ReadStderr(ctx context.Context, req *pb.ReadStreamRequest) (*pb.ReadStreamResponse, error) {
	data, err := a.sandbox.readStdio(req.ContainerId, req.ExecId, int(req.Len), false)
	if err != nil {
		return &pb.ReadStreamResponse{}, err
	}

	return &pb.ReadStreamResponse{
		Data: data,
	}, nil
}

func (a *agentGRPC) CloseStdin(ctx context.Context, req *pb.CloseStdinRequest) (*gpb.Empty, error) {
	proc, _, err := a.sandbox.getRunningProcess(req.ContainerId, req.ExecId)
	if err != nil {
		return emptyResp, err
	}

	var file *os.File
	if proc.termMaster != nil {
		file = proc.termMaster
	} else {
		file = proc.stdin
	}

	if err := file.Close(); err != nil {
		return emptyResp, err
	}

	return emptyResp, nil
}

func (a *agentGRPC) TtyWinResize(ctx context.Context, req *pb.TtyWinResizeRequest) (*gpb.Empty, error) {
	proc, _, err := a.sandbox.getRunningProcess(req.ContainerId, req.ExecId)
	if err != nil {
		return emptyResp, err
	}

	if proc.termMaster == nil {
		return emptyResp, fmt.Errorf("Terminal is not set, impossible to resize it")
	}

	winsize := &unix.Winsize{
		Row: uint16(req.Row),
		Col: uint16(req.Column),
	}

	// Set new terminal size.
	if err := unix.IoctlSetWinsize(int(proc.termMaster.Fd()), unix.TIOCSWINSZ, winsize); err != nil {
		return emptyResp, err
	}

	return emptyResp, nil
}

func (a *agentGRPC) CreateSandbox(ctx context.Context, req *pb.CreateSandboxRequest) (*gpb.Empty, error) {
	if a.sandbox.running == true {
		return emptyResp, fmt.Errorf("Sandbox already started, impossible to start again")
	}

	a.sandbox.id = req.Hostname
	a.sandbox.containers = make(map[string]*container)
	a.sandbox.network.dns = req.Dns
	a.sandbox.running = true

	if req.SandboxPidns {
		if err := a.sandbox.setupSharedPidNs(); err != nil {
			return emptyResp, err
		}
	}

	mountList, err := addMounts(req.Storages)
	if err != nil {
		return emptyResp, err
	}

	a.sandbox.mounts = mountList

	if err := setupDNS(a.sandbox.network.dns); err != nil {
		return emptyResp, err
	}

	return emptyResp, nil
}

func (a *agentGRPC) DestroySandbox(ctx context.Context, req *pb.DestroySandboxRequest) (*gpb.Empty, error) {
	if a.sandbox.running == false {
		agentLog.WithField("sandbox", a.sandbox.id).Info("Sandbox not started, this is a no-op")
		return emptyResp, nil
	}

	a.sandbox.Lock()
	for key, c := range a.sandbox.containers {
		if err := c.removeContainer(); err != nil {
			return emptyResp, err
		}

		delete(a.sandbox.containers, key)
	}
	a.sandbox.Unlock()

	if err := a.sandbox.removeNetwork(); err != nil {
		return emptyResp, err
	}

	if err := removeMounts(a.sandbox.mounts); err != nil {
		return emptyResp, err
	}

	if err := a.sandbox.teardownSharedPidNs(); err != nil {
		return emptyResp, err
	}

	a.sandbox.id = ""
	a.sandbox.containers = make(map[string]*container)
	a.sandbox.running = false
	a.sandbox.network = network{}
	a.sandbox.mounts = []string{}

	// Synchronize the caches on the system. This is needed to ensure
	// there is no pending transactions left before the VM is shut down.
	syscall.Sync()

	return emptyResp, nil
}

func (a *agentGRPC) AddInterface(ctx context.Context, req *pb.AddInterfaceRequest) (*gpb.Empty, error) {
	if err := a.sandbox.addInterface(nil, req.Interface); err != nil {
		return emptyResp, err
	}

	return emptyResp, nil
}

func (a *agentGRPC) RemoveInterface(ctx context.Context, req *pb.RemoveInterfaceRequest) (*gpb.Empty, error) {
	if err := a.sandbox.removeInterface(nil, req.Name); err != nil {
		return emptyResp, err
	}

	return emptyResp, nil
}

func (a *agentGRPC) UpdateInterface(ctx context.Context, req *pb.UpdateInterfaceRequest) (*gpb.Empty, error) {
	if err := a.sandbox.updateInterface(nil, req.Interface, req.Type); err != nil {
		return emptyResp, err
	}

	return emptyResp, nil
}

func (a *agentGRPC) AddRoute(ctx context.Context, req *pb.RouteRequest) (*gpb.Empty, error) {
	if err := a.sandbox.addRoute(nil, req.Route); err != nil {
		return emptyResp, err
	}

	return emptyResp, nil
}

func (a *agentGRPC) RemoveRoute(ctx context.Context, req *pb.RouteRequest) (*gpb.Empty, error) {
	if err := a.sandbox.removeRoute(nil, req.Route); err != nil {
		return emptyResp, err
	}

	return emptyResp, nil
}

func (a *agentGRPC) OnlineCPUMem(ctx context.Context, req *pb.OnlineCPUMemRequest) (*gpb.Empty, error) {
	go onlineCPUMem()

	return emptyResp, nil
}
