package store

import "context"

const maintenanceReviewActiveSQL = `SELECT 1 FROM review_run WHERE session_id=? AND status='running' LIMIT 1`
const maintenanceConversationLiveSQL = `SELECT 1 FROM conversation_turns WHERE conversation_id=? AND state IN ('queued','running') LIMIT 1`

// Only the private snapshot raw stage uses this transaction-local cache. Raw
// writes touch events/retention markers, never reviews or turn lifecycle. No
// payload, marker, CAS result, error, or fact from a different transaction is kept.
type maintenanceSnapshotFacts struct {
	maintenanceDB
	reviewActive     map[string]bool
	conversationLive map[string]bool
}

func maintenanceSnapshotEligibility(db maintenanceDB, stage string, snapshot ...bool) maintenanceDB {
	if stage != "raw" || len(snapshot) == 0 || !snapshot[0] {
		return db
	}
	return &maintenanceSnapshotFacts{db, map[string]bool{}, map[string]bool{}}
}

func maintenanceReviewActive(ctx context.Context, db maintenanceDB, owner string) (bool, error) {
	if facts, ok := db.(*maintenanceSnapshotFacts); ok {
		return facts.exists(ctx, facts.reviewActive, maintenanceReviewActiveSQL, owner)
	}
	return maintenanceExists(ctx, db, maintenanceReviewActiveSQL, owner)
}

func maintenanceConversationLive(ctx context.Context, db maintenanceDB, conversation string) (bool, error) {
	if facts, ok := db.(*maintenanceSnapshotFacts); ok {
		return facts.exists(ctx, facts.conversationLive, maintenanceConversationLiveSQL, conversation)
	}
	return maintenanceExists(ctx, db, maintenanceConversationLiveSQL, conversation)
}

func (facts *maintenanceSnapshotFacts) exists(ctx context.Context, values map[string]bool, query, key string) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if value, ok := values[key]; ok {
		return value, nil
	}
	value, err := maintenanceExists(ctx, facts.maintenanceDB, query, key)
	if err == nil {
		values[key] = value
	}
	return value, err
}
