package hsm

import (
	"fmt"
	"os"
	"sync"

	"github.intel.com/hpdd/logging/alert"
	"github.intel.com/hpdd/logging/debug"
	"github.intel.com/hpdd/lustre/fs"
	"golang.org/x/sys/unix"
)

// ActionSource is a source of HSM actions
type ActionSource interface {
	// Actions is a channel for HSM actions. Mutiple listeners can use this
	// channel.
	// The channel will be closed when the ActionSource is shutdown.
	Actions() <-chan ActionRequest

	// Stop signals the action source to shutdown. In-progress actions
	// will fail.
	Stop()
}

type coordinatorSource struct {
	fsRoot  fs.RootDir
	actions <-chan ActionRequest
	mu      sync.Mutex // Protect stopFd
	stopFd  *os.File
}

// Start initializes a coordinatorSource for the filesystem in root.
func Start(root fs.RootDir) (ActionSource, error) {
	src := &coordinatorSource{fsRoot: root}
	src.mu.Lock()
	defer src.mu.Unlock()
	// This pipe is used by Stop() to send the terminate signal to actionListener.
	r, w, err := os.Pipe()
	if err != nil {
		return nil, err
	}
	src.stopFd = w
	err = src.actionListener(r)
	if err != nil {
		return nil, err
	}
	return src, nil
}

func (src *coordinatorSource) Stop() {
	src.mu.Lock()
	defer src.mu.Unlock()
	if src.stopFd == nil {
		return
	}
	src.stopFd.Write([]byte("stop")) // Aribitrary data to wake up listener
	src.stopFd.Close()
	src.stopFd = nil
}

func (src *coordinatorSource) Actions() <-chan ActionRequest {
	return src.actions
}

func getFd(f *os.File) int {
	return int(f.Fd())
}

func (src *coordinatorSource) actionListener(stopFile *os.File) error {
	var err error
	cdc, err := NewCoordinatorClient(src.fsRoot, true)
	if err != nil {
		return fmt.Errorf("%s: %s", src.fsRoot, err)
	}

	ch := make(chan ActionRequest)

	go func() {
		var events = make([]unix.EpollEvent, 2)
		var ev unix.EpollEvent
		epfd, err := unix.EpollCreate1(unix.EPOLL_CLOEXEC)
		if err != nil {
			alert.Fatal(err)
		}
		ev.Fd = int32(getFd(stopFile))
		ev.Events = unix.EPOLLIN | unix.EPOLLET
		err = unix.EpollCtl(epfd, unix.EPOLL_CTL_ADD, getFd(stopFile), &ev)

		ev.Fd = int32(cdc.GetFd())
		ev.Events = unix.EPOLLIN | unix.EPOLLET
		err = unix.EpollCtl(epfd, unix.EPOLL_CTL_ADD, cdc.GetFd(), &ev)

		defer func() {
			cdc.Close()
			stopFile.Close()
			unix.Close(epfd)
			close(ch)
		}()

		for {
			var actions []*actionItem
			nfds, err := unix.EpollWait(epfd, events, -1)
			if err != nil {
				if err == unix.EINTR {
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
				case cdc.GetFd():
					for {
						actions, err = cdc.recv()
						if err == unix.EAGAIN {
							break
						}
						if err != nil {
							debug.Print(err)
							return
						}
						for _, ai := range actions {
							ch <- ai
						}
					}
				}

			}

		}
	}()

	src.actions = bufferedActionChannel(ch)
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
