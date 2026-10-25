package controllers

import (
 "context"
 "encoding/json"
 "errors"
 "io"
 "net"
 "net/http"
 "strings"

 "github.com/go-chi/chi/v5"
 "github.com/aoagents/agent-orchestrator/backend/internal/domain"
 "github.com/aoagents/agent-orchestrator/backend/internal/httpd/envelope"
 "github.com/aoagents/agent-orchestrator/backend/internal/httpd/requestscope"
)

type PreservedQueueRecoveryRequest = domain.PreservedQueueRecoveryRequest
type PreservedQueueRecoveryResponse = domain.PreservedQueueRecoveryResult

type preservedQueueRecoveryService interface {RecoverPreservedQueue(context.Context,domain.SessionID,domain.PreservedQueueRecoveryRequest)(domain.PreservedQueueRecoveryResult,error)}

func(c *ConversationsController) recoverPreservedQueue(w http.ResponseWriter,r *http.Request){
 // /dev is physically blocked before auth on every LAN/native listener. This
 // second check is context-owned; a reverse proxy's 127.0.0.1 peer is not proof.
 if requestscope.IsLAN(r.Context()) || !localQueueRecoveryRequest(r){
  envelope.WriteAPIError(w,r,http.StatusForbidden,"forbidden","LOCAL_MAINTENANCE_ONLY","local maintenance listener required",nil);return
 }
 svc,ok:=c.Svc.(preservedQueueRecoveryService);if !ok{envelope.WriteAPIError(w,r,http.StatusNotImplemented,"not_implemented","QUEUE_RECOVERY_UNAVAILABLE","queue recovery unavailable",nil);return}
 var req PreservedQueueRecoveryRequest
 dec:=json.NewDecoder(http.MaxBytesReader(w,r.Body,1<<20));dec.DisallowUnknownFields()
 if err:=dec.Decode(&req);err!=nil{envelope.WriteAPIError(w,r,http.StatusBadRequest,"bad_request","INVALID_JSON","invalid recovery metadata",nil);return}
 if err:=dec.Decode(new(any));err!=io.EOF{envelope.WriteAPIError(w,r,http.StatusBadRequest,"bad_request","INVALID_JSON","one recovery request required",nil);return}
 session:=domain.SessionID(chi.URLParam(r,"sessionId"))
 if _,err:=domain.PreservedQueueRecoveryDigest(session,req);err!=nil{envelope.WriteAPIError(w,r,http.StatusBadRequest,"bad_request","INVALID_RECOVERY_PROOF","explicit complete recovery proof required",nil);return}
 result,err:=svc.RecoverPreservedQueue(r.Context(),session,req)
 if err!=nil{
  if errors.Is(err,domain.ErrPreservedQueueRecovery){envelope.WriteAPIError(w,r,http.StatusConflict,"conflict","QUEUE_RECOVERY_CONFLICT","recovery proof no longer matches",nil);return}
  envelope.WriteAPIError(w,r,http.StatusConflict,"conflict","QUEUE_RECOVERY_NOT_READY","original live owner is not ready for recovery",nil);return
 }
 envelope.WriteJSON(w,http.StatusOK,result)
}
func localQueueRecoveryRequest(r *http.Request)bool{
 if r.Header.Get("Origin")!="" || r.Header.Get("Forwarded")!=""{return false}
 for name:=range r.Header{if strings.HasPrefix(strings.ToLower(name),"x-forwarded-"){return false}}
 host:=r.Host;if h,_,err:=net.SplitHostPort(host);err==nil{host=h}
 if host!="localhost" {ip:=net.ParseIP(strings.Trim(host,"[]"));if ip==nil || !ip.IsLoopback(){return false}}
 peer,_,err:=net.SplitHostPort(r.RemoteAddr);if err!=nil{return false};ip:=net.ParseIP(peer);return ip!=nil && ip.IsLoopback()
}
