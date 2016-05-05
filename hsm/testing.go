package hsm

import (
	"fmt"
	"math/rand"
	"time"

	"golang.org/x/net/context"

	"github.intel.com/hpdd/logging/debug"
	"github.intel.com/hpdd/lustre"
	"github.intel.com/hpdd/lustre/llapi"
)

type (
	// TestSource implements hsm.ActionSource, but provides a
	// Lustre-independent way of generating hsm requests.
	TestSource struct {
		outgoing   chan ActionRequest
		nextAction chan ActionRequest
		rng        *rand.Rand
	}

	testRequest struct {
		archive uint
		action  llapi.HsmAction
		testFid *lustre.Fid
	}

	testHandle struct {
		req ActionRequest
		fid *lustre.Fid
	}
)

// NewTestSource returns an ActionSource implementation suitable for testing
func NewTestSource() *TestSource {
	return &TestSource{
		nextAction: make(chan ActionRequest),
		outgoing:   make(chan ActionRequest),
		rng:        rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

// AddAction allows test code to inject arbitrary ActionRequests.
func (s *TestSource) AddAction(ar ActionRequest) {
	s.nextAction <- ar
}

// GenerateRandomAction generates a random action request
func (s *TestSource) GenerateRandomAction() {
	s.nextAction <- &testRequest{}
}

// Actions returns a channel for callers to receive ActionRequests
func (s *TestSource) Actions() <-chan ActionRequest {
	return s.outgoing
}

func (s *TestSource) run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			debug.Print("Shutting down test action generator")
			close(s.outgoing)
			return
		case next := <-s.nextAction:
			s.outgoing <- next
		}
	}
}

// Start starts the action generator
func (s *TestSource) Start(ctx context.Context) error {
	go s.run(ctx)

	// Bit of magic to let the test harness know that things are
	// started up.
	if signalFn, ok := ctx.Value("startSignal").(func()); ok {
		signalFn()
	}
	return nil
}

// NewTestRequest returns a new *testRequest
func NewTestRequest(archive uint, action llapi.HsmAction, fid *lustre.Fid) ActionRequest {
	return &testRequest{
		testFid: fid,
		archive: archive,
		action:  action,
	}
}

func (r *testRequest) Begin(flags int, isError bool) (ActionHandle, error) {
	return &testHandle{
		req: r,
		fid: r.testFid,
	}, nil
}

func (r *testRequest) FailImmediately(errval int) {
	return
}

func (r *testRequest) ArchiveID() uint {
	return r.archive
}

func (r *testRequest) String() string {
	return fmt.Sprintf("Test Request: %s", r.Action())
}

func (r *testRequest) Action() llapi.HsmAction {
	return r.action
}

func (h *testHandle) Progress(offset, length, total uint64, flags int) error {
	return nil
}

func (h *testHandle) End(offset, length uint64, flags int, errval int) error {
	return nil
}

func (h *testHandle) Action() llapi.HsmAction {
	return h.req.Action()
}

func (h *testHandle) Fid() *lustre.Fid {
	return h.fid
}

func (h *testHandle) Cookie() uint64 {
	return 0
}

func (h *testHandle) DataFid() (*lustre.Fid, error) {
	return h.fid, nil
}

func (h *testHandle) Fd() (int, error) {
	return 0, nil
}

func (h *testHandle) Offset() uint64 {
	return 0
}

func (h *testHandle) ArchiveID() uint {
	return h.req.ArchiveID()
}

func (h *testHandle) Length() uint64 {
	return 0
}

func (h *testHandle) String() string {
	return ""
}

func (h *testHandle) Data() []byte {
	return nil
}
