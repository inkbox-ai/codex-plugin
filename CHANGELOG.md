# Changelog

## 0.2.11

### Added

- Companion mode initializes sponsored email, MMS, and supported group iMessage conversations with one complete history input before processing live messages.
- Durable conversation queues recover pending history loading and pause uncertain Codex submissions for review.
- Conversation replies preserve their email reply-all parent or text conversation ID. Only a live sponsor can answer approvals in a sponsored session.

### Changed

- Requires Inkbox SDK >=0.7.3,<1.0.0. Updating the bridge does not enable Companion mode on an identity.
