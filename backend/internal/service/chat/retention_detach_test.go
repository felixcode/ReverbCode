package chat_test

import (
 "context"
 "crypto/sha256"
 "fmt"
 "log/slog"
 "reflect"
 "sync/atomic"
 "testing"
 "time"

 "github.com/aoagents/agent-orchestrator/backend/internal/domain"
 "github.com/aoagents/agent-orchestrator/backend/internal/ports"
 chatsvc "github.com/aoagents/agent-orchestrator/backend/internal/service/chat"
 "github.com/aoagents/agent-orchestrator/backend/internal/storage/sqlite/store"
)

type retentionReceiptReconnect struct{*liveReconnectedConversation; ack atomic.Bool}
func(c *retentionReceiptReconnect) AcknowledgeProviderEvent(_ context.Context,id string)error{if id=="original-complete"{c.ack.Store(true)};return nil}

func TestRetentionDetachAdoptPreservesRunningAndThirtyEightQueued(t *testing.T){
 ctx:=context.Background();st:=openStore(t);reader:=fullSnapshotReader(st);var ids atomic.Int32
 newID:=func()string{return fmt.Sprintf("retention-adopt-%d",ids.Add(1))}
 firstProvider:=&terminatingConversation{fakeConversation:newFakeConversation()}
 first:=chatsvc.New(chatsvc.Options{Store:st,Reader:reader,Sessions:st,Drivers:fakeRegistry{driver:fakeDriver{conv:firstProvider}},Log:slog.New(slog.DiscardHandler),NewID:newID})
 firstCtrl,err:=first.Start(ctx,chatsvc.StartConfig{SessionID:testSession,ProjectID:testProject,Harness:domain.HarnessCodex,WorkspacePath:t.TempDir()});if err!=nil{t.Fatal(err)}
 running,err:=firstCtrl.Send(ctx,ports.ChatUserMessage{Text:"keep original active prompt",ClientMessageID:"original-client"});if err!=nil{t.Fatal(err)}
 firstProvider.emit(ports.ChatEvent{Kind:ports.ChatEventTurnStarted,ProviderTurnID:running.ProviderTurnID})
 h:=&harness{st:st,ctrl:firstCtrl};h.awaitSnapshot(t,func(s store.ConversationSnapshot)bool{return len(s.Turns)==1&&s.Turns[0].State==domain.TurnStateRunning})
 for i:=0;i<38;i++{queued,e:=firstCtrl.Send(ctx,ports.ChatUserMessage{Text:fmt.Sprintf("queued input %d",i),ClientMessageID:fmt.Sprintf("q-%d",i)});if e!=nil||queued.State!=domain.TurnStateQueued{t.Fatalf("queue %d: %v",i,e)}}
 type proof struct{state domain.TurnState;digest [32]byte}
 capture:=func(s store.ConversationSnapshot)map[string]proof{out:=map[string]proof{};for _,turn:=range s.Turns{p:=proof{state:turn.State};for _,m:=range s.Messages{if m.TurnID==turn.ID&&m.Role==domain.MessageRoleUser{p.digest=sha256.Sum256([]byte(m.ID+"\x00"+m.ClientMessageID+"\x00"+m.Text))}};out[turn.ID]=p};return out}
 before,err:=st.LoadConversationSnapshot(ctx,firstCtrl.ConversationID());if err!=nil{t.Fatal(err)};expected:=capture(before)
 first.StopAll(ctx)
 detached,err:=st.LoadConversationSnapshot(ctx,firstCtrl.ConversationID());if err!=nil{t.Fatal(err)}
 if firstProvider.terminated.Load()||!reflect.DeepEqual(expected,capture(detached)){t.Fatal("detach changed durable queue/input ownership")}
 native:=&nativeHistoryConversation{fakeConversation:newFakeConversation()};native.turnSeq=1
 provider:=&retentionReceiptReconnect{liveReconnectedConversation:&liveReconnectedConversation{nativeHistoryConversation:native}}
 var sentBeforeAck atomic.Bool;native.onSend=func(string){if !provider.ack.Load(){sentBeforeAck.Store(true)}}
 second:=chatsvc.New(chatsvc.Options{Store:st,Reader:reader,Sessions:st,Drivers:fakeRegistry{driver:fakeDriver{conv:provider}},Log:slog.New(slog.DiscardHandler),NewID:newID});defer second.StopAll(ctx)
 ctrl,err:=second.Start(ctx,chatsvc.StartConfig{SessionID:testSession,ProjectID:testProject,Harness:domain.HarnessCodex,WorkspacePath:t.TempDir(),ProviderConversationID:firstProvider.ProviderConversationID()});if err!=nil{t.Fatal(err)}
 adopted,err:=st.LoadConversationSnapshot(ctx,ctrl.ConversationID());if err!=nil{t.Fatal(err)}
 if !reflect.DeepEqual(expected,capture(adopted))||ctrl.State()!=ports.ChatControllerBusy||native.historyReads()!=0||len(native.sentTexts())!=0{t.Fatal("live adopter replayed a prompt or changed queue")}
 native.emit(ports.ChatEvent{Kind:ports.ChatEventTurnCompleted,ProviderTurnID:running.ProviderTurnID,ProviderEventID:"original-complete",TurnState:domain.TurnStateCompleted})
 deadline:=time.Now().Add(3*time.Second);for len(native.sentTexts())==0&&time.Now().Before(deadline){time.Sleep(5*time.Millisecond)}
 if sentBeforeAck.Load()||!provider.ack.Load()||len(native.sentTexts())!=1{t.Fatal("surviving completion did not ACK then dispatch exactly one queue item")}
}
