package hsm

import (
	"fmt"
	"os"
	"sync"
	"syscall"

	"github.intel.com/hpdd/logging/alert"
	"github.intel.com/hpdd/logging/debug"
	"github.intel.com/hpdd/lustre/fs"
)

// Agent receives HSM action  from the Coordinator.
type Agent interface {
	// Actions is a channel for actions. Mutiple listeners can use this channel.
	// The channel will be closed when the Agent is shutdown.
	Actions() <-chan ActionRequest

	// Stop signals the agent to shutdown. It disconnects from the coordinator and
	// in progress actions will fail.
	Stop()
}

type agent struct {
	root    fs.RootDir
	actions <-chan ActionRequest
	mu      sync.Mutex // Protect stopFd
	stopFd  *os.File
}

// Start initializes an agent for the filesystem in root.
func Start(root fs.RootDir) (Agent, error) {
	agent := &agent{root: root}
	agent.mu.Lock()
	defer agent.mu.Unlock()
	// This pipe is used by Stop() to send the terminate signal to actionListener.
	r, w, err := os.Pipe()
	if err != nil {
		return nil, err
	}
	agent.stopFd = w
	err = agent.actionListener(r)
	if err != nil {
		return nil, err
	}
	return agent, nil
}

func (agent *agent) Stop() {
	agent.mu.Lock()
	defer agent.mu.Unlock()
	if agent.stopFd == nil {
		return
	}
	agent.stopFd.Write([]byte("stop")) // Aribitrary data to wake up listener
	agent.stopFd.Close()
	agent.stopFd = nil
}

func (agent *agent) Actions() <-chan ActionRequest {
	return agent.actions
}
func getFd(f *os.File) int {
	return int(f.Fd())
}

// EPOLLET is defined here because syscall.EPOLLET overflows uint32
// https://github.com/golang/go/issues/5328
const EPOLLET = uint32(1) << 31

func (agent *agent) actionListener(stopFile *os.File) error {
	var err error
	cdt, err := CoordinatorConnection(agent.root, true)
	if err != nil {
		return fmt.Errorf("%s: %s", agent.root, err)
	}

	ch := make(chan ActionRequest)

	go func() {
		var events = make([]syscall.EpollEvent, 2)
		var ev syscall.EpollEvent
		epfd, err := syscall.EpollCreate1(syscall.EPOLL_CLOEXEC)
		if err != nil {
			alert.Fatal(err)
		}
		ev.Fd = int32(getFd(stopFile))
		ev.Events = syscall.EPOLLIN | EPOLLET
		err = syscall.EpollCtl(epfd, syscall.EPOLL_CTL_ADD, getFd(stopFile), &ev)

		ev.Fd = int32(cdt.GetFd())
		ev.Events = syscall.EPOLLIN | EPOLLET
		err = syscall.EpollCtl(epfd, syscall.EPOLL_CTL_ADD, cdt.GetFd(), &ev)

		defer func() {
			cdt.Close()
			stopFile.Close()
			syscall.Close(epfd)
			close(ch)
		}()

		for {
			var actions []*ActionItem
			nfds, err := syscall.EpollWait(epfd, events, -1)
			if err != nil {
				if err == syscall.Errno(syscall.EINTR) {
					continue
				}
				alert.Fatal(err)
			}

			for n := 0; n < nfds; n++ {
				ev := events[n]
				switch int(ev.Fd) {
				case getFd(stopFile):
					buf := make([]byte, 32)
					stopFile.Read(buf)
					return
				case cdt.GetFd():
					actions, err = cdt.Recv()
					if err != nil {
						debug.Print(err)
						return
					}
				}

			}

			for _, ai := range actions {
				ch <- ai
			}
		}
	}()

	agent.actions = bufferedActionChannel(ch)
	return nil
}

// bufferedActionChannel buffers the input channel into an arbitrarily sized queue, and returns
// the channel for consumers to read from.
func bufferedActionChannel(in <-chan ActionRequest) <-chan ActionRequest {
	var queue []ActionRequest
	out := make(chan ActionRequest)

	go func() {
		defer close(out)
		for {
			var send chan ActionRequest
			var first ActionRequest

			if len(queue) > 0 {
				send = out
				first = queue[0]
			}
			select {
			case item, ok := <-in:
				if !ok {
					debug.Print("in channel failed, close out!")
					return
				}
				queue = append(queue, item)

			case send <- first:
				queue = queue[1:]
			}
		}
	}()

	return out
}
