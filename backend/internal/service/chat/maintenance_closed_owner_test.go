package chat
import("testing";"github.com/aoagents/agent-orchestrator/backend/internal/domain")
func TestClosedOwnerMaintenanceGuardNeverWaitsOnBusinessLocks(t *testing.T){
 s:=&Service{};id:=domain.SessionID("owner")
 if !s.ClosedOwnerMaintenanceInactive(id){t.Fatal("empty registry not inactive")}
 s.mu.Lock();if s.ClosedOwnerMaintenanceInactive(id){t.Fatal("busy registry allowed")};s.mu.Unlock()
 s.wakeMu.Lock();if s.ClosedOwnerMaintenanceInactive(id){t.Fatal("busy wake allowed")};s.wakeMu.Unlock()
 s.gateMu.Lock();if s.ClosedOwnerMaintenanceInactive(id){t.Fatal("busy start gate allowed")};s.gateMu.Unlock()
}
func TestClosedOwnerMaintenanceGuardRetainsControllerReviewerAndWake(t *testing.T){
 id:=domain.SessionID("owner");s:=&Service{controllers:map[domain.SessionID]*Controller{id:{}},startConfigs:map[domain.ConversationOwner]StartConfig{},waking:map[domain.SessionID]int{},gates:map[domain.ConversationOwner]controllerGate{}}
 if s.ClosedOwnerMaintenanceInactive(id){t.Fatal("live controller allowed")};delete(s.controllers,id)
 reviewer:=domain.ReviewConversationOwner("review");s.startConfigs[reviewer]=StartConfig{SessionID:id};if s.ClosedOwnerMaintenanceInactive(id){t.Fatal("reviewer controller allowed")};delete(s.startConfigs,reviewer)
 s.waking[id]=1;if s.ClosedOwnerMaintenanceInactive(id){t.Fatal("pending wake allowed")};delete(s.waking,id)
 gate:=make(controllerGate,1);gate<-struct{}{};s.gates[domain.SessionConversationOwner(id)]=gate;if s.ClosedOwnerMaintenanceInactive(id){t.Fatal("start in progress allowed")};<-gate
 if !s.ClosedOwnerMaintenanceInactive(id){t.Fatal("settled registry not inactive")}
}
