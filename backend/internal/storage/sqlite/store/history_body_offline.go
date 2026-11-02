package store
import("context";"time")
type historyBodyCoverage struct{Before time.Time;MaxEventID int64}
type historyBodyCoverageKey struct{}
// No SQL/body values enter this scope. Mutable rows newer than the already
// verified recovery point stay intact; event IDs use the covered append prefix.
func historyBodyCoverageReason(ctx context.Context,row maintenanceRow,stage string)string{
 c,ok:=ctx.Value(historyBodyCoverageKey{}).(historyBodyCoverage);if !ok{return ""}
 if stage=="raw"||stage=="gc:raw"{if row.integer("id")>c.MaxEventID{return "offline_event_after_backup"};return ""}
 if stage=="turn"{return "offline_turn_mutable_coverage_unknown"}
 t,e:=maintenanceTime(row.text("updated_at"));if e!=nil{return "offline_backup_timestamp_unknown"};if t.After(c.Before){return "offline_updated_after_backup"};return ""
}
func historyRawBodyCanExpire(method string)bool{return historyGCStreamMethod(method)||method=="message.completed"||method=="turn.diff"}
func historyBodyAlreadyEmpty(row maintenanceRow,stage string)bool{
 switch stage{
 case "outputs":return row.text("command_output")==""&&row.text("streamed_text")==""
 case "activity":return (row.text("summary")==""||row.text("summary")==maintenanceExpired)&&(row.text("detail_json")==""||row.text("detail_json")==`{"_ao_history_expired":true}`)&&row.text("command_output")==""&&row.text("streamed_text")==""
 case "message":return (row.text("text")==""||row.text("text")==maintenanceExpired)&&row.text("delivery_content_json")==""
 case "turn":return row.text("diff_json")==""&&row.text("plan_json")==""&&(row.text("error_message")==""||row.text("error_message")==maintenanceExpired||row.text("error_message")=="controller ended before the turn completed")
 case "session":return row.text("prompt")==""||row.text("prompt")==maintenanceExpired
 };return false
}

func historyBodyValueMarker(value any)bool{
 switch v:=value.(type){case string:return v==maintenanceExpired;case map[string]any:return len(v)==1&&v["_ao_history_expired"]==true};return false
}
