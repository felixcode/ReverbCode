package store
import "testing"
func TestRawCacheWriterProtectionIsRequestScoped(t *testing.T) {
 first:=HistoryMaintenanceResult{}
 first.rawCacheWriterProtection()["conv"]=historyGCProtection{unknown:true}
 if !first.rawCacheWriterProtection()["conv"].unknown { t.Fatal("writer did not share same-conversation protection") }
 next:=HistoryMaintenanceResult{}
 if len(next.rawCacheWriterProtection())!=0 { t.Fatal("protection leaked into next writer transaction") }
}
