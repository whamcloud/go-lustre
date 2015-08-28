package hsm

import (
	"fmt"
	"log"
	"os"
	"syscall"

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
	stopFd  *os.File
	actions <-chan ActionRequest
}

// Start initializes an agent for the filesystem in root.
func Start(root fs.RootDir, done chan struct{}) (Agent, error) {
	agent := &agent{root: root}

	// This pipe is used by Stop() to signal the action waiter goroutine.
	r, w, err := os.Pipe()
	if err != nil {
		return nil, err
	}
	agent.stopFd = w
	err = agent.launchActionWaiter(r, done)
	if err != nil {
		return nil, err
	}
	return agent, nil
}

func (agent *agent) Stop() {
	// TODO: lock agent
	if agent == nil || agent.stopFd == nil {
		return
	}
	agent.stopFd.Write([]byte("stop"))
	agent.stopFd.Close()
	agent.stopFd = nil
}

func (agent *agent) Actions() <-chan ActionRequest {
	return agent.actions
}
func getFd(f *os.File) int {
	return int(f.Fd())
}

// the version in syscall is missing the uint32 and doesn't compile here
const EPOLLET = uint32(1) << 31

func (agent *agent) launchActionWaiter(r *os.File, done chan struct{}) error {
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
			log.Fatal(err)
		}
		ev.Fd = int32(getFd(r))
		ev.Events = syscall.EPOLLIN | EPOLLET
		err = syscall.EpollCtl(epfd, syscall.EPOLL_CTL_ADD, getFd(r), &ev)

		ev.Fd = int32(cdt.GetFd())
		ev.Events = syscall.EPOLLIN | EPOLLET
		err = syscall.EpollCtl(epfd, syscall.EPOLL_CTL_ADD, cdt.GetFd(), &ev)

		defer func() {
			cdt.Close()
			close(ch)
			r.Close()
			syscall.Close(epfd)
		}()

		for {
			var actions []ActionItem
			nfds, err := syscall.EpollWait(epfd, events, -1)
			if err != nil {
				if err == syscall.Errno(syscall.EINTR) {
					continue
				}
				log.Fatal(err)
			}

			for n := 0; n < nfds; n++ {
				ev := events[n]
				switch int(ev.Fd) {
				case getFd(r):
					buf := make([]byte, 32)
					r.Read(buf)
					// might be better to fall throuhg and exit at done below, but don't
					// want to risk starting new actions when we're about to quit
					return
				case cdt.GetFd():
					actions, err = cdt.Recv()
					if err != nil {
						log.Println(err)
						return
					}
				}

			}

			select {
			case <-done:
				log.Println("actionWaiter done")
				return
			default:
			}
			for _, ai := range actions {
				a := ai
				ch <- &a
			}
		}
	}()

	agent.actions = bufferedActionChannel(done, ch)
	return nil
}

// bufferedActionChannel buffers the input channel into an arbitrarily sized queue, and returns
// the channel for consumers to read from.
func bufferedActionChannel(done <-chan struct{}, in <-chan ActionRequest) <-chan ActionRequest {
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
					log.Println("in channel failed, close out!")
					return
				}
				queue = append(queue, item)

			case send <- first:
				queue = queue[1:]

			case <-done:
				log.Println("buffered channel done")

				return
			}
		}
	}()

	return out
}
