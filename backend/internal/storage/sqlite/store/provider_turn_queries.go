package store

import (
	"context"
	"github.com/aoagents/agent-orchestrator/backend/internal/storage/sqlite/gen"
)

// Empty legacy provider IDs retain the original query semantics. Nonempty IDs
// make the existing partial unique index eligible without changing tx ownership.
func providerTurnSelectConversationTurnByProviderID(ctx context.Context, q *gen.Queries, arg gen.SelectConversationTurnByProviderIDParams) (gen.ConversationTurn, error) {
	if arg.ProviderTurnID != "" {
		return q.SelectConversationTurnByProviderIDNonempty(ctx, gen.SelectConversationTurnByProviderIDNonemptyParams(arg))
	}
	return q.SelectConversationTurnByProviderID(ctx, arg)
}
func providerTurnUpdateConversationTurnDiff(ctx context.Context, q *gen.Queries, arg gen.UpdateConversationTurnDiffParams) (int64, error) {
	if arg.ProviderTurnID != "" {
		return q.UpdateConversationTurnDiffNonempty(ctx, gen.UpdateConversationTurnDiffNonemptyParams(arg))
	}
	return q.UpdateConversationTurnDiff(ctx, arg)
}
func providerTurnUpdateConversationTurnPlan(ctx context.Context, q *gen.Queries, arg gen.UpdateConversationTurnPlanParams) (int64, error) {
	if arg.ProviderTurnID != "" {
		return q.UpdateConversationTurnPlanNonempty(ctx, gen.UpdateConversationTurnPlanNonemptyParams(arg))
	}
	return q.UpdateConversationTurnPlan(ctx, arg)
}
