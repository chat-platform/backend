# Scope and Assumptions

## 1. Scope

### In Scope

- User registration and authentication
- 1-to-1 messaging
- Group messaging
- Message delivery and read status
- Online/offline presence
- Message history
- Media messages
- Push notifications
- Multi-device support

### Out of Scope

- Voice calls
- Video calls
- Payments
- Stories/status
- Channels
- Bots
- Business accounts

---

## 2. Assumptions

- Users may frequently disconnect and reconnect.
- Mobile users may be offline for extended periods.
- Messages should be persisted for offline recipients.
- Messages can be edited only by the author, from the device they were originally sent from. (This has design implications with respect to ordering.)
- Messages can be edited only within 15 minutes of sending.
- Delete for all: The author can delete their message within one hour. The client enforces this limit; the server verifies authorship and message existence. The revoke is queued for current chat members' non-revoked devices with an unexpired session; group admins cannot revoke another member's message.
- Delete for me: Messages will be deleted only on the device where the user performs the deletion; the deletion will not be reflected on their other logged-in devices.
- Logged-out sessions cannot receive revokes; local messages are cleared on logout. Restoring revoked content from future backups is outside scope.
- For direct and group chats, messages are acknowledged but discarded without persistence or idempotency when no eligible recipient devices exist.

### Message Retention Distribution

| Retention Time | % of Pending Messages |
|---|---:|
| Within 1 hour | 50% |
| Within 1 day | 30% |
| Within 7 days | 15% |
| Within 30 days | 5% |
| **Total** | **100%** |

=> Average pending-message retention: 3.1days

---

## 3. Constraints

- This is a learning and portfolio project, not a production system.
- Infrastructure and operational costs should remain reasonable.
- The design will avoid unnecessary complexity while still demonstrating
  important distributed-system concepts.
