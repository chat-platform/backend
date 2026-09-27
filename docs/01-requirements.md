# 01 — Requirements

## 1. Functional Requirements

### 1.1 User Management
- Users can create an account.
- Users can log in and log out.
- Users can manage their profile.

### 1.2 Messaging
- Users can send 1-to-1 messages.
- Users can receive messages.
- Users can retrieve message history.
- Users can delete messages.

### 1.3 Conversations
- Users can start a conversation.
- Users can create group conversations.
- Users can add/remove group members.

### 1.4 Message Status
- A message can have a sent status.
- A message can have a delivered status.
- A message can have a read status.

### 1.5 Presence
- Users can see whether another user is online.
- Users can see last-seen information.

### 1.6 Media
- Users can send images.
- Users can send videos.
- Users can send documents.
- Users can send voice messages.

### 1.7 Notifications
- Users receive notifications for new messages when appropriate.

### 1.8 Devices
- Users can use multiple devices.
- Users can be logged in from multiple devices simultaneously.
- Users can manage their connected devices.

---
## 2. Non-Functional Requirements

### 2.1 Availability
The system should remain available despite individual component failures.

Detailed analysis: [Reliability](./08-reliability.md)

### 2.2 Performance
Messages should be delivered with low latency under normal operating conditions.

Detailed analysis: [Scale Estimation](./03-scale-estimation.md)

### 2.3 Scalability
The system should be capable of scaling as the number of users and messages grows.

Detailed analysis: [Scale Estimation](./03-scale-estimation.md)

### 2.4 Reliability
The system should avoid message loss and handle failures gracefully.

Detailed analysis: [Reliability](./08-reliability.md)

### 2.5 Security
The system should protect user accounts, messages, and personal data.

Detailed analysis: [Security](./09-security.md)

### 2.6 Consistency
The system should define appropriate consistency guarantees for messages,
message ordering, delivery status, and presence.

Detailed analysis: [Data Model](./05-data-model.md) / [Detailed Design](./07-detailed-design/)