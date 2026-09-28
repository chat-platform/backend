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