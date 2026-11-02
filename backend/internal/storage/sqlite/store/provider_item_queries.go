package store

import (
	"context"

	"github.com/aoagents/agent-orchestrator/backend/internal/storage/sqlite/gen"
)

// Provider item IDs can be empty in legacy inputs. Route only nonempty IDs to
// the partial-index queries; preserve the original empty-ID matching semantics.
func providerItemAppendConversationMessageDelta(ctx context.Context, q *gen.Queries, arg gen.AppendConversationMessageDeltaParams) error {
	if arg.ProviderItemID != "" {
		return q.AppendConversationMessageDeltaNonempty(ctx, gen.AppendConversationMessageDeltaNonemptyParams(arg))
	}
	return q.AppendConversationMessageDelta(ctx, arg)
}

func providerItemSettleConversationMessage(ctx context.Context, q *gen.Queries, arg gen.SettleConversationMessageParams) error {
	if arg.ProviderItemID != "" {
		return q.SettleConversationMessageNonempty(ctx, gen.SettleConversationMessageNonemptyParams(arg))
	}
	return q.SettleConversationMessage(ctx, arg)
}

func providerItemSelectConversationMessageByProviderItem(ctx context.Context, q *gen.Queries, arg gen.SelectConversationMessageByProviderItemParams) (gen.ConversationMessage, error) {
	if arg.ProviderItemID != "" {
		return q.SelectConversationMessageByProviderItemNonempty(ctx, gen.SelectConversationMessageByProviderItemNonemptyParams(arg))
	}
	return q.SelectConversationMessageByProviderItem(ctx, arg)
}

func providerItemAppendConversationActivityOutput(ctx context.Context, q *gen.Queries, arg gen.AppendConversationActivityOutputParams) (int64, error) {
	if arg.ProviderItemID != "" {
		return q.AppendConversationActivityOutputNonempty(ctx, gen.AppendConversationActivityOutputNonemptyParams(arg))
	}
	return q.AppendConversationActivityOutput(ctx, arg)
}

func providerItemAppendConversationActivityStreamedText(ctx context.Context, q *gen.Queries, arg gen.AppendConversationActivityStreamedTextParams) (int64, error) {
	if arg.ProviderItemID != "" {
		return q.AppendConversationActivityStreamedTextNonempty(ctx, gen.AppendConversationActivityStreamedTextNonemptyParams(arg))
	}
	return q.AppendConversationActivityStreamedText(ctx, arg)
}

func providerItemConversationActivityExistsForProviderItem(ctx context.Context, q *gen.Queries, arg gen.ConversationActivityExistsForProviderItemParams) (bool, error) {
	if arg.ProviderItemID != "" {
		return q.ConversationActivityExistsForProviderItemNonempty(ctx, gen.ConversationActivityExistsForProviderItemNonemptyParams(arg))
	}
	return q.ConversationActivityExistsForProviderItem(ctx, arg)
}
