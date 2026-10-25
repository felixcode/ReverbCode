package domain

import (
 "crypto/sha256"
 "encoding/hex"
 "encoding/json"
 "errors"
 "slices"
 "strings"
)

// ErrPreservedQueueRecovery refuses an unproven or changed recovery batch.
var ErrPreservedQueueRecovery = errors.New("preserved_queue_recovery_conflict")

type PreservedQueueInputProof struct {
 ID string `json:"id"`
 ClientMessageID string `json:"clientMessageId"`
 ClientPayloadHash string `json:"clientPayloadHash"`
 TextSHA256 string `json:"textSha256"`
 DeliverySHA256 string `json:"deliverySha256"`
}
type PreservedQueueTurnProof struct {
 ID string `json:"id"`
 PreviousGeneration string `json:"previousGeneration"`
 RequestedAt string `json:"requestedAt"`
 BeforeState string `json:"beforeState"`
 Inputs []PreservedQueueInputProof `json:"inputs"`
}
type PreservedQueueRecoveryRequest struct {
 Protocol int `json:"protocol"`
 ConversationID string `json:"conversationId"`
 BranchID string `json:"branchId"`
 NativeID string `json:"nativeId"`
 BoundTurnID string `json:"boundTurnId"`
 BoundProviderTurnID string `json:"boundProviderTurnId"`
 FailureAt string `json:"failureAt"`
 BeforeSnapshotSHA256 string `json:"beforeSnapshotSha256"`
 Turns []PreservedQueueTurnProof `json:"turns"`
}
type PreservedQueueRecoveryResult struct {
 Restored int `json:"restored"`
 PreviouslyRestored int `json:"previouslyRestored"`
 RequestSHA256 string `json:"requestSha256"`
 Dispatch string `json:"dispatch"`
}

// PreservedQueueRecoveryDigest canonicalizes metadata only, never prompt text.
func PreservedQueueRecoveryDigest(session SessionID, req PreservedQueueRecoveryRequest) (string,error) {
 fail:=func()(string,error){return "",ErrPreservedQueueRecovery}
 if req.Protocol!=1 || session=="" || req.ConversationID=="" || req.BranchID=="" || req.NativeID=="" || req.BoundTurnID=="" || req.BoundProviderTurnID=="" || req.FailureAt=="" || !recoveryHash(req.BeforeSnapshotSHA256) || len(req.Turns)==0 || len(req.Turns)>4096 {return fail()}
 req.Turns=slices.Clone(req.Turns)
 seen:=map[string]bool{};inputs:=map[string]bool{}
 for i:=range req.Turns {
  t:=&req.Turns[i]
  if t.ID=="" || t.ID==req.BoundTurnID || seen[t.ID] || t.PreviousGeneration=="" || t.RequestedAt=="" || t.BeforeState!="queued" || len(t.Inputs)==0 {return fail()};seen[t.ID]=true
  t.Inputs=slices.Clone(t.Inputs)
  for _,m:=range t.Inputs {if m.ID=="" || inputs[m.ID] || !((m.ClientMessageID=="" && m.ClientPayloadHash=="") || (m.ClientMessageID!="" && recoveryHash(m.ClientPayloadHash))) || !recoveryHash(m.TextSHA256) || !recoveryHash(m.DeliverySHA256) {return fail()};inputs[m.ID]=true}
  slices.SortFunc(t.Inputs,func(a,b PreservedQueueInputProof)int{return strings.Compare(a.ID,b.ID)})
 }
 slices.SortFunc(req.Turns,func(a,b PreservedQueueTurnProof)int{return strings.Compare(a.ID,b.ID)})
 data,err:=json.Marshal(struct{Session SessionID `json:"session"`;Request PreservedQueueRecoveryRequest `json:"request"`}{session,req});if err!=nil{return "",err}
 sum:=sha256.Sum256(data);return hex.EncodeToString(sum[:]),nil
}
func recoveryHash(value string) bool {decoded,err:=hex.DecodeString(value);return err==nil && len(decoded)==sha256.Size && value==strings.ToLower(value)}
