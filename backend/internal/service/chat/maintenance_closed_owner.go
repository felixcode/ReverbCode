package chat

import "github.com/aoagents/agent-orchestrator/backend/internal/domain"

// ClosedOwnerMaintenanceInactive never waits on a business lock and never
// calls the Store. Unknown/starting/stopping ownership is conservatively live.
func(s *Service) ClosedOwnerMaintenanceInactive(id domain.SessionID)bool{
 if !s.mu.TryRLock(){return false};defer s.mu.RUnlock()
 if s.controllers[id]!=nil{return false}
 for _,cfg:=range s.startConfigs{if cfg.SessionID==id||cfg.SessionID==""{return false}}
 if !s.wakeMu.TryLock(){return false};defer s.wakeMu.Unlock()
 if s.waking[id]>0||s.wakeRuns[id]!=nil||s.backgroundWakes[id]{return false}
 if !s.gateMu.TryLock(){return false};defer s.gateMu.Unlock()
 return len(s.gates[domain.SessionConversationOwner(id)])==0
}
