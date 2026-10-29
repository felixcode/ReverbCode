package store

import (
 "context"
 "database/sql"
 "errors"
 "strconv"
 "unicode/utf8"
)

// Indexed MAX/last-key metadata only. Bounds restrict reader selection; they
// never replace writer qualification, CAS, recovery checks or cutoff policy.
func(s *Store) HistoryMaintenancePassBounds(ctx context.Context)(map[string]string,error){
 var event int64
 if e:=s.readDB.QueryRowContext(ctx,`SELECT COALESCE(MAX(id),0) FROM conversation_provider_events`).Scan(&event);e!=nil{return nil,e}
 var activity sql.NullString
 if e:=s.readDB.QueryRowContext(ctx,`SELECT MAX(id) FROM conversation_activities`).Scan(&activity);e!=nil{return nil,e}
 var audit historyAuditKey
 e:=s.readDB.QueryRowContext(ctx,`SELECT object_kind,object_id FROM ao_history_retention ORDER BY object_kind DESC,object_id DESC LIMIT 1`).Scan(&audit[0],&audit[1])
 if e!=nil&&!errors.Is(e,sql.ErrNoRows){return nil,e}
 if len(activity.String)>8192||len(audit.String())>8192||!utf8.ValidString(activity.String)||!utf8.ValidString(audit[0])||!utf8.ValidString(audit[1]){return nil,errors.New("invalid_pass_bound_utf8")}
 eventID:=strconv.FormatInt(event,10)
 return map[string]string{"gc:raw":eventID,"cache:raw":eventID,"gc:closed-raw":eventID,"gc:activity":activity.String,"gc:closed-activity":activity.String,"gc:audit":audit.String()},nil
}
