package httpd

import (
 "encoding/json"
 "net/http"
 "net/http/httptest"
 "os"
 "testing"

 "github.com/aoagents/agent-orchestrator/backend/internal/config"
)

func TestRetentionCapabilityRequiresExplicitRuntimeWiring(t *testing.T){
 for _,enabled:=range []bool{false,true}{
 control:=ControlDeps{}
 if enabled{control.HistoryRetention=func()map[string]any{return map[string]any{"protocol":1,"schemaReady":true,"dbDevice":uint64(2),"dbInode":uint64(3),"processStartTicks":uint64(4)}}}
 router:=NewRouterWithControl(config.Config{},discardLogger(),nil,APIDeps{},control)
 recorder:=httptest.NewRecorder();router.ServeHTTP(recorder,httptest.NewRequest(http.MethodGet,"/healthz",nil))
 var payload map[string]any;if err:=json.Unmarshal(recorder.Body.Bytes(),&payload);err!=nil{t.Fatal(err)}
 if payload["pid"]!=float64(os.Getpid()){t.Fatal("capability lacks running process identity")}
 _,exists:=payload["historyRetention"];if exists!=enabled{t.Fatal("table existence/legacy wiring must not advertise compatibility")}
 }
}
