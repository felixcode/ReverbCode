#!/usr/bin/env bash
set -euo pipefail
umask 077

# Test-only entrypoint for a disposable no-network container. No source/candidate
# DB argument exists: every database/host/protocol input comes from test fixtures.
if [[ $# -ne 1 || "$1" != /* ]]; then
  echo 'usage: recovery-readiness.sh /absolute/new-output-directory' >&2
  exit 2
fi
out="$1"
mkdir -m 700 "$out"
export GOTMPDIR="$out/go-tmp"
mkdir -m 700 "$GOTMPDIR"
export AO_ACP_NOTIFICATION_QUEUE_CAPACITY=65536
export AO_ACP_NOTIFICATION_QUEUE_BYTES=134217728
export AO_REQUEST_TIMEOUT=5m
cd "$(dirname "$0")/../../backend"

go build -mod=readonly -p 1 -o "$out/ao" ./cmd/ao
sha256sum "$out/ao" > "$out/binary.sha256"
export AO_RECOVERY_SMOKE_BINARY="$out/ao"

go test -mod=readonly -p 1 -count=1 -timeout 180s -json ./internal/maintenance/rowidcopy \
  -run '^(TestRebuiltApplicationStoreKeepsOriginalQueuedInputAndNewIdentity|TestRebuiltEmptyApplicationDaemonBootSmoke)$' > "$out/rebuilt-boot.jsonl"
go test -mod=readonly -p 1 -count=1 -timeout 180s -json ./internal/adapters/chatdriver/acp \
  -run '^(TestACPReplayLoadLargeHistoryThroughSDK|TestReconnectReplayWaitsForConsumer|TestReconnectReplaySlowConsumer|TestReconnectReplayUnsubscribedCloseReleasesReader|TestReconnectRecoveryPendingTerminalKeepsOriginalTurn)$' > "$out/acp-protocol.jsonl"
go test -mod=readonly -p 1 -count=1 -timeout 180s -json ./internal/adapters/chatdriver/codexappserver \
  -run '^(TestNativeForkHandoffRetainsEachExchangeOnce|TestReconnectAdoptsInitializedHostWithoutNativeResume|TestReconnectMissingHostNeverLaunchesProvider|TestSendTurnCarriesIdempotencyKey)$' > "$out/codex-protocol.jsonl"
go test -mod=readonly -p 1 -count=1 -timeout 180s -json ./internal/storage/sqlite/store \
  -run '^(TestArchivedIDsExactGenerationAndProjectionTransaction|TestRecoverLiveTransportTurnRequiresExactJournalArchiveOwnership|TestAppendUserMessageRejectsChangedPayloadForSameClientID)$' > "$out/durable-identities.jsonl"
go test -mod=readonly -p 1 -count=1 -timeout 180s -json ./internal/service/chat \
  -run '^(TestServiceLiveTransportRecoveryDoesNotRepeatPrompt|TestDuplicateSendDoesNotCreateASecondTurn|TestReconcileNativeHistoryUpgradesRecoveredWithKnownProviderOutcome|TestReconcileNativeHistoryPreservesKnownOutcomeOverRecoveredReplay|TestNativeHistorySettleDelayBacksOffToCap)$' > "$out/controller-recovery.jsonl"
go test -mod=readonly -p 1 -count=1 -timeout 180s -json ./internal/daemon \
  -run '^(TestPersistentChatHostKeepSetUsesDurableOwnership|TestFreshChatStartWaitsForPersistentHostReconcile)$' > "$out/startup-gates.jsonl"
