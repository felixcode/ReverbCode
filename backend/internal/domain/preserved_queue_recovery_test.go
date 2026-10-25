package domain

import (
 "strings"
 "testing"
)
func queueProofRequest()PreservedQueueRecoveryRequest{
 h:=strings.Repeat("a",64)
 return PreservedQueueRecoveryRequest{Protocol:1,ConversationID:"conversation",BranchID:"branch",NativeID:"native",BoundTurnID:"bound",BoundProviderTurnID:"provider-bound",FailureAt:"exact-old-failure",BeforeSnapshotSHA256:h,Turns:[]PreservedQueueTurnProof{{ID:"queued",PreviousGeneration:"old",RequestedAt:"original-order",BeforeState:"queued",Inputs:[]PreservedQueueInputProof{{ID:"original-message",TextSHA256:h,DeliverySHA256:h}}}}}
}
func TestPreservedQueueRecoveryLegacyProofAndCanonicalDigest(t *testing.T){
 req:=queueProofRequest();hash,err:=PreservedQueueRecoveryDigest("session",req);if err!=nil||len(hash)!=64{t.Fatal("legacy proof rejected")}
 req.Turns[0].Inputs[0].ClientMessageID="client"
 if _,err:=PreservedQueueRecoveryDigest("session",req);err==nil{t.Fatal("mixed empty identity accepted")}
 req.Turns[0].Inputs[0].ClientPayloadHash=strings.Repeat("b",64)
 if _,err:=PreservedQueueRecoveryDigest("session",req);err!=nil{t.Fatal("complete client identity rejected")}
 req.Turns[0].Inputs[0].TextSHA256=""
 if _,err:=PreservedQueueRecoveryDigest("session",req);err==nil{t.Fatal("missing body proof accepted")}
}
