package bot

import (
	"fmt"
	"strings"

	"github.com/bwmarrin/discordgo"
)

const (
	// DefaultWebhookName is the standard name used when creating channel webhooks for persona posting.
	DefaultWebhookName = "WackyPub Agent"
)

// EnsureWebhook finds an existing WackyPub webhook in the channel, or creates a new one.
// Returns nil if the bot does not have permissions to manage webhooks or if the channel is a DM.
func EnsureWebhook(s *discordgo.Session, channelID string) (*discordgo.Webhook, error) {
	// Webhooks cannot be created in DMs
	ch, err := s.Channel(channelID)
	if err != nil {
		return nil, fmt.Errorf("failed to get channel info: %w", err)
	}
	if ch.Type == discordgo.ChannelTypeDM || ch.Type == discordgo.ChannelTypeGroupDM {
		return nil, nil
	}

	webhooks, err := s.ChannelWebhooks(channelID)
	if err == nil {
		for _, wh := range webhooks {
			if wh != nil && wh.Name == DefaultWebhookName && wh.Token != "" {
				return wh, nil
			}
		}
	}

	// Create new webhook
	wh, err := s.WebhookCreate(channelID, DefaultWebhookName, "")
	if err != nil {
		return nil, err
	}
	return wh, nil
}

// SendAgentMessage delivers a message from an agent to a Discord channel.
// It uses channel webhooks for persona/custom username impersonation when available,
// and gracefully falls back to standard bot message delivery when webhooks are disabled or fail.
func SendAgentMessage(s *discordgo.Session, channelID string, agentID string, content string, webhook *discordgo.Webhook) error {
	chunks := SplitDiscordMessage(content)
	if len(chunks) == 0 {
		return nil
	}

	username := agentID
	if len(username) > 0 {
		username = strings.ToUpper(username[:1]) + username[1:]
	}

	for _, chunk := range chunks {
		sent := false
		if webhook != nil && webhook.ID != "" && webhook.Token != "" {
			params := &discordgo.WebhookParams{
				Content:  chunk,
				Username: username,
			}
			_, err := s.WebhookExecute(webhook.ID, webhook.Token, true, params)
			if err == nil {
				sent = true
			}
		}

		if !sent {
			// Fallback to standard message send
			if _, err := s.ChannelMessageSend(channelID, chunk); err != nil {
				return fmt.Errorf("failed to send message to channel %s: %w", channelID, err)
			}
		}
	}

	return nil
}
