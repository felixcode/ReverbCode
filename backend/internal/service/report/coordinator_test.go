package report

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
)

func TestCoordinatorFixedFallbackAndSettlementBatch(t *testing.T) {
	now := time.Date(2026, 9, 14, 10, 0, 0, 0, time.UTC)
	store := newCoordinatorStore(now)
	store.sessions = []domain.SessionRecord{{ID: "orch", ProjectID: "p", Kind: domain.KindOrchestrator, Mode: domain.SessionModeChat}}
	store.add(reportAt("one", "worker", now, now.Add(time.Hour), "first"))
	store.add(reportAt("two", "worker", now.Add(20*time.Minute), now.Add(80*time.Minute), "second"))
	delivery := &coordinatorDelivery{}
	c := NewCoordinator(CoordinatorDeps{Store: store, Delivery: delivery, Now: func() time.Time { return now }, NewToken: func() string { return "claim" }})

	if delay, err := c.RunDue(context.Background()); err != nil || delay != time.Hour {
		t.Fatalf("before fallback delay = %v, err = %v", delay, err)
	}
	now = now.Add(time.Hour)
	if _, err := c.RunDue(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(delivery.submissions) != 1 || !strings.Contains(delivery.submissions[0].message, "first") || !strings.Contains(delivery.submissions[0].message, "second") {
		t.Fatalf("submission = %#v, want one shared batch", delivery.submissions)
	}
	if store.acked != 2 {
		t.Fatalf("acknowledged = %d, want 2", store.acked)
	}

	store.add(reportAt("done-1", "worker", now, now.Add(5*time.Minute), "done"))
	store.add(reportAt("done-2", "worker", now.Add(2*time.Minute), now.Add(7*time.Minute), "later done"))
	now = now.Add(5 * time.Minute)
	if _, err := c.RunDue(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(delivery.submissions) != 2 || !strings.Contains(delivery.submissions[1].message, "later done") {
		t.Fatalf("settlement submission = %#v", delivery.submissions)
	}
}

func TestCoordinatorRoutesAtAttemptAndLeavesPendingWithoutOrchestrator(t *testing.T) {
	now := time.Date(2026, 9, 14, 11, 0, 0, 0, time.UTC)
	store := newCoordinatorStore(now)
	store.add(reportAt("need", "worker", now, now, "decision"))
	delivery := &coordinatorDelivery{}
	c := NewCoordinator(CoordinatorDeps{Store: store, Delivery: delivery, Now: func() time.Time { return now }, RetryDelay: time.Second})
	if delay, err := c.RunDue(context.Background()); err == nil || delay != time.Second {
		t.Fatalf("no orchestrator delay = %v, err = %v", delay, err)
	}
	if store.reports[0].DeliveryState != domain.ReportPending {
		t.Fatalf("state = %s, want pending", store.reports[0].DeliveryState)
	}
	now = now.Add(time.Second) // respect the durable failure backoff
	store.sessions = []domain.SessionRecord{
		{ID: "old", ProjectID: "p", Kind: domain.KindOrchestrator, Mode: domain.SessionModeChat, IsTerminated: true, CreatedAt: now},
		{ID: "replacement", ProjectID: "p", Kind: domain.KindOrchestrator, Mode: domain.SessionModeChat, CreatedAt: now.Add(time.Second)},
	}
	if _, err := c.RunDue(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(delivery.submissions) != 1 || delivery.submissions[0].target != "replacement" {
		t.Fatalf("submissions = %#v", delivery.submissions)
	}
}

func TestCoordinatorStuckInterruptRateLimitAndCoalescing(t *testing.T) {
	now := time.Date(2026, 9, 14, 12, 0, 0, 0, time.UTC)
	store := newCoordinatorStore(now)
	store.sessions = []domain.SessionRecord{{ID: "orch", ProjectID: "p", Kind: domain.KindOrchestrator, Mode: domain.SessionModeChat}}
	first := reportAt("stuck-1", "worker", now, now, "cannot continue")
	first.State = domain.ReportStuck
	second := reportAt("stuck-2", "worker", now.Add(time.Second), now, "cannot continue")
	second.State = domain.ReportStuck
	store.add(first, second)
	delivery := &coordinatorDelivery{}
	c := NewCoordinator(CoordinatorDeps{Store: store, Delivery: delivery, Now: func() time.Time { return now }})
	if _, err := c.RunDue(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(delivery.interrupts) != 1 {
		t.Fatalf("interrupts = %v", delivery.interrupts)
	}
	if !strings.Contains(delivery.submissions[0].message, "repeated 2 times") {
		t.Fatalf("message = %q", delivery.submissions[0].message)
	}
	repeat := reportAt("stuck-3", "worker", now.Add(time.Minute), now, "cannot continue")
	repeat.State = domain.ReportStuck
	store.add(repeat)
	if _, err := c.RunDue(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(delivery.interrupts) != 1 {
		t.Fatalf("rate limited interrupts = %v", delivery.interrupts)
	}
	if len(delivery.submissions) != 1 {
		t.Fatalf("throttled repeat created an extra wakeup: %#v", delivery.submissions)
	}
}

func TestCoordinatorAcceptanceAndPiggybackFencing(t *testing.T) {
	now := time.Date(2026, 9, 14, 13, 0, 0, 0, time.UTC)
	store := newCoordinatorStore(now)
	store.sessions = []domain.SessionRecord{{ID: "orch", ProjectID: "p", Kind: domain.KindOrchestrator, Mode: domain.SessionModeChat}}
	store.add(reportAt("info", "worker", now, now.Add(time.Hour), "context"))
	c := NewCoordinator(CoordinatorDeps{Store: store, Delivery: &coordinatorDelivery{}, Now: func() time.Time { return now }, NewToken: func() string { return "piggyback-token" }})
	batch, err := c.PreparePiggyback(context.Background(), "orch")
	if err != nil || len(batch.Reports) != 1 {
		t.Fatalf("prepare = %#v, err = %v", batch, err)
	}
	combined := batch.AppendToUserMessage("hello")
	if !strings.HasPrefix(combined, "hello\n\n<ao-worker-reports>\n") ||
		!strings.HasSuffix(combined, "\n</ao-worker-reports>") {
		t.Fatalf("piggyback text = %q", combined)
	}
	if store.acked != 0 {
		t.Fatal("claimed report acknowledged before acceptance")
	}
	if err := c.AcceptPiggyback(context.Background(), batch); err != nil {
		t.Fatal(err)
	}
	if store.acked != 1 {
		t.Fatalf("acknowledged = %d", store.acked)
	}
	if count, _ := store.AcknowledgeReportBatch(context.Background(), "p", "stale-token", now); count != 0 {
		t.Fatalf("stale token acknowledged %d reports", count)
	}
}

type coordinatorStore struct {
	mu         sync.Mutex
	now        time.Time
	reports    []domain.ReportRecord
	sessions   []domain.SessionRecord
	interrupts map[domain.SessionID]time.Time
	acked      int
}

func newCoordinatorStore(now time.Time) *coordinatorStore {
	return &coordinatorStore{now: now, interrupts: map[domain.SessionID]time.Time{}}
}

func reportAt(id string, worker domain.SessionID, created, available time.Time, note string) domain.ReportRecord {
	return domain.ReportRecord{ID: id, SessionID: worker, ProjectID: "p", State: domain.ReportCheckpoint, Note: note, CreatedAt: created, AvailableAt: available, DeliveryState: domain.ReportPending, RepeatCount: 1}
}

func (s *coordinatorStore) add(records ...domain.ReportRecord) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.reports = append(s.reports, records...)
}
func (s *coordinatorStore) ListSessions(context.Context, domain.ProjectID) ([]domain.SessionRecord, error) {
	return append([]domain.SessionRecord(nil), s.sessions...), nil
}
func (s *coordinatorStore) GetSession(_ context.Context, id domain.SessionID) (domain.SessionRecord, bool, error) {
	for _, rec := range s.sessions {
		if rec.ID == id {
			return rec, true, nil
		}
	}
	return domain.SessionRecord{}, false, nil
}
func (s *coordinatorStore) ListPendingReportSchedule(context.Context, int64) ([]domain.ReportRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var pending []domain.ReportRecord
	for _, rec := range s.reports {
		if rec.DeliveryState == domain.ReportPending {
			pending = append(pending, rec)
		}
	}
	sortReports(pending)
	if len(pending) > 1 {
		pending = pending[:1]
	}
	return pending, nil
}
func (s *coordinatorStore) ClaimPendingReportBatch(_ context.Context, project domain.ProjectID, token string, at time.Time) ([]domain.ReportRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	batchID := ""
	for i := range s.reports {
		if s.reports[i].ProjectID == project && s.reports[i].DeliveryState == domain.ReportPending {
			batchID = s.reports[i].DeliveryBatchID
			if batchID == "" {
				batchID = "report-batch:" + s.reports[i].ID
			}
			break
		}
	}
	var claimed []domain.ReportRecord
	for i := range s.reports {
		if s.reports[i].ProjectID == project && s.reports[i].DeliveryState == domain.ReportPending {
			if s.reports[i].DeliveryBatchID != "" && s.reports[i].DeliveryBatchID != batchID {
				continue
			}
			s.reports[i].DeliveryBatchID = batchID
			s.reports[i].DeliveryAttempts++
			s.reports[i].DeliveryState, s.reports[i].ClaimToken, s.reports[i].ClaimedAt = domain.ReportClaimed, token, at
			claimed = append(claimed, s.reports[i])
		}
	}
	sortReports(claimed)
	return claimed, nil
}
func (s *coordinatorStore) AcknowledgeReportBatch(_ context.Context, project domain.ProjectID, token string, at time.Time) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	count := 0
	for i := range s.reports {
		if s.reports[i].ProjectID == project && s.reports[i].DeliveryState == domain.ReportClaimed && s.reports[i].ClaimToken == token {
			s.reports[i].DeliveryState, s.reports[i].AcknowledgedAt = domain.ReportAcknowledged, at
			count++
		}
	}
	s.acked += count
	return count, nil
}
func (s *coordinatorStore) ReleaseReportBatch(_ context.Context, project domain.ProjectID, token string, _ time.Time, _ string) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	count := 0
	for i := range s.reports {
		if s.reports[i].ProjectID == project && s.reports[i].DeliveryState == domain.ReportClaimed && s.reports[i].ClaimToken == token {
			s.reports[i].DeliveryState, s.reports[i].ClaimToken = domain.ReportPending, ""
			count++
		}
	}
	return count, nil
}
func (s *coordinatorStore) DeferReportBatch(ctx context.Context, project domain.ProjectID, token string, at time.Time, reason string) (int, error) {
	count, err := s.ReleaseReportBatch(ctx, project, token, at, reason)
	if err != nil {
		return count, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for i := range s.reports {
		if s.reports[i].ProjectID == project && s.reports[i].DeliveryState == domain.ReportPending && s.reports[i].DeliveryBatchID != "" && s.reports[i].AvailableAt.Before(at) {
			s.reports[i].AvailableAt = at
		}
	}
	return count, nil
}
func (s *coordinatorStore) AcquireReportInterrupt(_ context.Context, worker domain.SessionID, at time.Time, window time.Duration) (bool, time.Time, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	last, ok := s.interrupts[worker]
	if ok && last.After(at.Add(-window)) {
		return false, last.Add(window), nil
	}
	s.interrupts[worker] = at
	return true, at, nil
}

func sortReports(records []domain.ReportRecord) {
	for i := 1; i < len(records); i++ {
		for j := i; j > 0 && (records[j].AvailableAt.Before(records[j-1].AvailableAt) || (records[j].AvailableAt.Equal(records[j-1].AvailableAt) && records[j].ID < records[j-1].ID)); j-- {
			records[j], records[j-1] = records[j-1], records[j]
		}
	}
}

type coordinatorSubmission struct {
	target       domain.SessionID
	message, key string
}
type coordinatorDelivery struct {
	submissions []coordinatorSubmission
	interrupts  []domain.SessionID
	submitErr   error
}

func (d *coordinatorDelivery) Submit(_ context.Context, target domain.SessionID, message, key string) error {
	if d.submitErr != nil {
		return d.submitErr
	}
	d.submissions = append(d.submissions, coordinatorSubmission{target, message, key})
	return nil
}
func (d *coordinatorDelivery) Interrupt(_ context.Context, target domain.SessionID) error {
	d.interrupts = append(d.interrupts, target)
	return nil
}

func TestCoordinatorRetryBackoffSurvivesRestartAndWake(t *testing.T) {
	now := time.Date(2026, 10, 9, 8, 0, 0, 0, time.UTC)
	st := newCoordinatorStore(now)
	st.sessions = []domain.SessionRecord{{ID: "orch", ProjectID: "p", Kind: domain.KindOrchestrator, Mode: domain.SessionModeChat}}
	st.add(reportAt("retry", "worker", now, now, "pending work"))
	d := &coordinatorDelivery{submitErr: errors.New("provider unavailable")}
	fresh := func() *Coordinator {
		return NewCoordinator(CoordinatorDeps{Store: st, Delivery: d, Now: func() time.Time { return now }})
	}
	c := fresh()
	for _, wait := range []time.Duration{5 * time.Second, 10 * time.Second, 20 * time.Second, 40 * time.Second, time.Minute, time.Minute} {
		if _, err := c.RunDue(context.Background()); err == nil {
			t.Fatal("failure was acknowledged")
		}
		if got := st.reports[0].AvailableAt.Sub(now); got != wait {
			t.Fatalf("retry delay=%v, want %v", got, wait)
		}
		if st.acked != 0 {
			t.Fatal("failed batch acknowledged")
		}
		// A new coordinator (daemon restart) and Wake must keep the durable deadline.
		c = fresh()
		c.Wake()
		if delay, err := c.RunDue(context.Background()); err != nil || delay != wait {
			t.Fatalf("restart delay=%v err=%v", delay, err)
		}
		now = now.Add(wait)
	}
	d.submitErr = nil
	if _, err := c.RunDue(context.Background()); err != nil {
		t.Fatal(err)
	}
	if st.acked != 1 || len(d.submissions) != 1 || d.submissions[0].key != "report-batch:retry" {
		t.Fatalf("recovery ack=%d deliveries=%v", st.acked, d.submissions)
	}
}

func TestCoordinatorBlockedCurrentDoesNotWakeOlderOrchestrator(t *testing.T) {
	for _, state := range []domain.ActivityState{domain.ActivityBlocked, domain.ActivityWaitingInput} {
		t.Run(string(state), func(t *testing.T) {
			now := time.Now().UTC()
			st := newCoordinatorStore(now)
			st.sessions = []domain.SessionRecord{
				{ID: "older", Kind: domain.KindOrchestrator, ProjectID: "p", CreatedAt: now.Add(-time.Hour)},
				{ID: "current", Kind: domain.KindOrchestrator, ProjectID: "p", CreatedAt: now, Activity: domain.Activity{State: state}},
			}
			st.add(reportAt("blocked", "worker", now, now, "pending"))
			d := &coordinatorDelivery{}
			c := NewCoordinator(CoordinatorDeps{Store: st, Delivery: d})
			if _, err := c.RunDue(context.Background()); err == nil {
				t.Fatal("blocked orchestrator was receptive")
			}
			if len(d.submissions) != 0 || st.acked != 0 {
				t.Fatal("blocked current redirected to older orchestrator")
			}
		})
	}
}

type notifiedDelivery struct{ submitted chan coordinatorSubmission }

func (d *notifiedDelivery) Submit(_ context.Context, id domain.SessionID, msg, key string) error {
	d.submitted <- coordinatorSubmission{id, msg, key}
	return nil
}
func (*notifiedDelivery) Interrupt(context.Context, domain.SessionID) error { return nil }

func TestCoordinatorLowFrequencyRecoversMissedWakeWithoutEmptyTurns(t *testing.T) {
	now := time.Now().UTC()
	st := newCoordinatorStore(now)
	st.sessions = []domain.SessionRecord{{ID: "orch", Kind: domain.KindOrchestrator, ProjectID: "p"}}
	// A future report must not hide a newly due report whose Wake was lost.
	st.add(reportAt("future", "worker", now, now.Add(time.Hour), "future"))
	d := &notifiedDelivery{submitted: make(chan coordinatorSubmission, 2)}
	c := NewCoordinator(CoordinatorDeps{Store: st, Delivery: d, ReconcileInterval: 10 * time.Millisecond})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := c.Start(ctx)
	select {
	case <-d.submitted:
		t.Fatal("future-only queue started an empty turn")
	case <-time.After(30 * time.Millisecond):
	}
	st.add(reportAt("missed", "worker", now, now, "real work")) // deliberately no Wake
	select {
	case got := <-d.submitted:
		if got.target != "orch" {
			t.Fatalf("target=%s", got.target)
		}
	case <-time.After(time.Second):
		t.Fatal("missed Wake was not recovered")
	}
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("coordinator failed to stop")
	}
	if len(d.submitted) != 0 {
		t.Fatal("duplicate delivery")
	}
	// No pending reports means checks perform no semantic sends.
	if delay, err := c.RunDue(context.Background()); err != nil || delay != -1 {
		t.Fatalf("idle delay=%v err=%v", delay, err)
	}
}

type failingAckStore struct {
	*coordinatorStore
	fail bool
}

func (s *failingAckStore) AcknowledgeReportBatch(ctx context.Context, project domain.ProjectID, token string, at time.Time) (int, error) {
	if s.fail {
		s.fail = false
		return 0, errors.New("acknowledgement write unavailable")
	}
	return s.coordinatorStore.AcknowledgeReportBatch(ctx, project, token, at)
}

func TestCoordinatorAcknowledgementFailureKeepsSameBatchRetryable(t *testing.T) {
	now := time.Now().UTC()
	st := &failingAckStore{coordinatorStore: newCoordinatorStore(now), fail: true}
	st.sessions = []domain.SessionRecord{{ID: "orch", ProjectID: "p", Kind: domain.KindOrchestrator}}
	st.add(reportAt("ack", "worker", now, now, "real work"))
	d := &coordinatorDelivery{}
	c := NewCoordinator(CoordinatorDeps{Store: st, Delivery: d, Now: func() time.Time { return now }})
	if _, err := c.RunDue(context.Background()); err == nil {
		t.Fatal("ack failure lost")
	}
	if st.reports[0].DeliveryState != domain.ReportPending || st.acked != 0 {
		t.Fatal("accepted batch stranded in claimed")
	}
	now = now.Add(5 * time.Second)
	if _, err := c.RunDue(context.Background()); err != nil {
		t.Fatal(err)
	}
	if st.acked != 1 || len(d.submissions) != 2 || d.submissions[0].key != d.submissions[1].key {
		t.Fatalf("retry changed identity: ack=%d deliveries=%v", st.acked, d.submissions)
	}
}
