package chat_test

import (
 "context"
 "crypto/sha256"
 "encoding/hex"
 "fmt"
 "log/slog"
 "sync/atomic"
 "testing"
 "time"

 "github.com/aoagents/agent-orchestrator/backend/internal/domain"
 "github.com/aoagents/agent-orchestrator/backend/internal/ports"
 chatsvc "github.com/aoagents/agent-orchestrator/backend/internal/service/chat"
 "github.com/aoagents/agent-orchestrator/backend/internal/storage/sqlite"
 "github.com/aoagents/agent-orchestrator/backend/internal/storage/sqlite/store"
)
type oldDeferredRecoveryConversation struct{*fakeConversation}
func(c *oldDeferredRecoveryConversation)StartDeferredTurn(string)error{return nil}
func(c *oldDeferredRecoveryConversation)DiscardDeferredTurn(string){}
type preservedQueueConversation struct{*transportRecoveryConversation;disconnected atomic.Bool}
func(c *preservedQueueConversation)StartDeferredTurn(string)error{return nil}
func(c *preservedQueueConversation)DiscardDeferredTurn(string){}
func(c *preservedQueueConversation)DisconnectedProviderState()ports.ChatDisconnectState{if c.disconnected.Load(){return ports.ChatDisconnectPreserved};return ports.ChatDisconnectNotPersistent}
func proofSHA(value string)string{sum:=sha256.Sum256([]byte(value));return hex.EncodeToString(sum[:])}
type interceptQueueRecoveryStore struct{*sqlite.Store;before func()}
func(s *interceptQueueRecoveryStore)RestorePreservedQueue(ctx context.Context,session domain.SessionID,gen string,req domain.PreservedQueueRecoveryRequest,ids []string,at time.Time)(domain.PreservedQueueRecoveryResult,error){if s.before!=nil{s.before()};return s.Store.RestorePreservedQueue(ctx,session,gen,req,ids,at)}
func queueServiceFixture(t *testing.T,wrap func(*sqlite.Store)chatsvc.Store)(*chatsvc.Service,*chatsvc.Controller,*preservedQueueConversation,*sqlite.Store,domain.PreservedQueueRecoveryRequest){
 t.Helper();st:=openStore(t);ctx:=context.Background();var n atomic.Int32;newID:=func()string{return fmt.Sprintf("queue-fixture-%d",n.Add(1))}
 rec,found,err:=st.GetSession(ctx,testSession);if err!=nil||!found{t.Fatal(err)};rec.Harness=domain.HarnessClaudeCode;if err=st.UpdateSession(ctx,rec);err!=nil{t.Fatal(err)}
 oldProvider:=&oldDeferredRecoveryConversation{newFakeConversation()}
 first:=chatsvc.New(chatsvc.Options{Store:st,Reader:fullSnapshotReader(st),Sessions:st,Drivers:fakeRegistry{driver:fakeDriver{conv:oldProvider}},NewID:newID,Log:slog.New(slog.DiscardHandler)})
 old,err:=first.Start(ctx,chatsvc.StartConfig{SessionID:testSession,ProjectID:testProject,Harness:domain.HarnessClaudeCode,WorkspacePath:t.TempDir()});if err!=nil{t.Fatal(err)}
 initial,err:=st.LoadConversationSnapshot(ctx,old.ConversationID());if err!=nil{t.Fatal(err)}
 if err=st.CreateAndActivateConversationBranch(ctx,testSession,domain.ConversationBranch{ID:"same-native-branch",ConversationID:old.ConversationID(),SessionID:testSession,ProviderConversationID:oldProvider.ProviderConversationID(),ParentBranchID:initial.Conversation.ActiveBranchID,CreatedAt:time.Now().UTC()},old.Generation(),time.Now().UTC());err!=nil{t.Fatal(err)}
 bound,err:=old.Send(ctx,ports.ChatUserMessage{Text:"original live prompt",ClientMessageID:"original-client"});if err!=nil{t.Fatal(err)}
 oldProvider.emit(ports.ChatEvent{Kind:ports.ChatEventTurnStarted,ProviderEventID:"opening:0",ProviderTurnID:bound.ProviderTurnID,ProviderConversationID:oldProvider.ProviderConversationID()})
 h:=&harness{st:st,ctrl:old};h.awaitSnapshot(t,func(s store.ConversationSnapshot)bool{return len(s.Turns)>0 && old.State()==ports.ChatControllerBusy})
 for i:=0;i<2;i++{if _,err=old.Send(ctx,ports.ChatUserMessage{Text:fmt.Sprintf("queued original %d",i),ClientMessageID:fmt.Sprintf("original-queued-client-%d",i)});err!=nil{t.Fatal(err)}}
 // Simulate the pre-fix cleanup, not a real current-user cancellation.
 oldProvider.Close();old.Wait()
 failed,err:=st.LoadConversationSnapshot(ctx,old.ConversationID());if err!=nil{t.Fatal(err)}
 req:=domain.PreservedQueueRecoveryRequest{Protocol:1,ConversationID:old.ConversationID(),BranchID:failed.Conversation.ActiveBranchID,NativeID:oldProvider.ProviderConversationID(),BoundTurnID:bound.ID,BoundProviderTurnID:bound.ProviderTurnID,BeforeSnapshotSHA256:proofSHA("original snapshot")}
 for _,turn:=range failed.Turns{if turn.ID==bound.ID{continue};proof:=domain.PreservedQueueTurnProof{ID:turn.ID,PreviousGeneration:old.Generation(),RequestedAt:turn.RequestedAt.String(),BeforeState:"queued"};req.FailureAt=turn.CompletedAt.String();for _,m:=range failed.Messages{if m.TurnID==turn.ID{proof.Inputs=append(proof.Inputs,domain.PreservedQueueInputProof{ID:m.ID,ClientMessageID:m.ClientMessageID,ClientPayloadHash:m.ClientPayloadHash,TextSHA256:proofSHA(m.Text),DeliverySHA256:proofSHA(m.DeliveryContentJSON)})}};req.Turns=append(req.Turns,proof)}
 provider:=&preservedQueueConversation{transportRecoveryConversation:&transportRecoveryConversation{liveReconnectedConversation:&liveReconnectedConversation{nativeHistoryConversation:&nativeHistoryConversation{fakeConversation:newFakeConversation()}},ids:[]string{"opening","terminal"}}};provider.turnSeq=100
 var storage chatsvc.Store=st;if wrap!=nil{storage=wrap(st)}
 svc:=chatsvc.New(chatsvc.Options{Store:storage,Reader:fullSnapshotReader(st),Sessions:st,Drivers:fakeRegistry{driver:fakeDriver{conv:provider}},NewID:newID,Log:slog.New(slog.DiscardHandler)})
 ctrl,err:=svc.Start(ctx,chatsvc.StartConfig{SessionID:testSession,ProjectID:testProject,Harness:domain.HarnessClaudeCode,WorkspacePath:t.TempDir(),ProviderConversationID:provider.ProviderConversationID()});if err!=nil{t.Fatal(err)}
 t.Cleanup(func(){svc.StopAll(context.Background())})
 return svc,ctrl,provider,st,req
}
func TestPreservedQueueRecoveryBusyNeverSendsAndCompletedUsesNormalDrain(t *testing.T){
 for _,terminal:=range []bool{false,true}{t.Run(fmt.Sprint(terminal),func(t *testing.T){
  svc,ctrl,provider,st,req:=queueServiceFixture(t,nil);ctx:=context.Background()
  if terminal{provider.emit(ports.ChatEvent{Kind:ports.ChatEventTurnCompleted,ProviderEventID:"terminal",ProviderConversationID:req.NativeID,ProviderTurnID:req.BoundProviderTurnID,TurnState:domain.TurnStateCompleted});h:=&harness{st:st,ctrl:ctrl};h.awaitSnapshot(t,func(snapshot store.ConversationSnapshot)bool{return ctrl.State()==ports.ChatControllerReady})}
  result,err:=svc.RecoverPreservedQueue(ctx,testSession,req);if err!=nil{t.Fatal(err)};if result.Restored!=2{t.Fatalf("counts %+v",result)}
  want:=0;if terminal{want=1};if provider.sendCallCount()!=want{t.Fatalf("provider calls=%d wanted=%d",provider.sendCallCount(),want)}
  repeat,err:=svc.RecoverPreservedQueue(ctx,testSession,req);if err!=nil||repeat.Restored!=0||repeat.PreviouslyRestored!=2{t.Fatalf("idempotence %+v %v",repeat,err)};if provider.sendCallCount()!=want{t.Fatal("repeat drained again")}
 })}
}
func TestPreservedQueueRecoveryGenerationRaceAndDisconnectedAreRejected(t *testing.T){
 for _,disconnected:=range []bool{false,true}{t.Run(fmt.Sprint(disconnected),func(t *testing.T){
  var wrapped *interceptQueueRecoveryStore
  svc,_,provider,st,req:=queueServiceFixture(t,func(s *sqlite.Store)chatsvc.Store{wrapped=&interceptQueueRecoveryStore{Store:s};return wrapped})
  if disconnected{provider.disconnected.Store(true)}else{wrapped.before=func(){if err:=st.ClaimChatControllerGeneration(context.Background(),testSession,"foreign-generation");err!=nil{t.Fatal(err)}}}
  if _,err:=svc.RecoverPreservedQueue(context.Background(),testSession,req);err==nil{t.Fatal("stale/disconnected accepted")};if provider.sendCallCount()!=0{t.Fatal("refusal sent input")}
  snapshot,err:=st.LoadConversationSnapshot(context.Background(),req.ConversationID);if err!=nil{t.Fatal(err)};for _,turn:=range snapshot.Turns{if turn.ProviderTurnID==""&&turn.State!=domain.TurnStateFailed{t.Fatal("partially recovered")}}
 })}
}
