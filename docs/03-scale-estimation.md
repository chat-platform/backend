# 03 — Scale Estimation

## 1. User Scale

- Registered users: 3.3B
- Daily active users (DAU): 2.3B
- Peak concurrent users: 300M

## 2. Messaging Scale

- Average messages per active user per day:  100
- Average messages per day: 230B
- Peak messages per second: 15M  <!-- ~ 300M peak concurrent users, let 1/20 be sending msg

## 3. Storage

- Average message size: 1 KB
- Size of messages in a a day: ~230 TB
- Maximum pending-message retention: 30 days
- Average pending-message retention: 3.1days
- Message storage required until delivery: ~700TB (=230TB*3.1)
- Media storage: ~2.3PB

## 4. Bandwidth

- Average message traffic: ~2.7 GB/s (~21 Gbps)
  <!-- 230B messages / 86,400s × 1 KB ≈ 2.66 GB/s ≈ 21 Gbps -->

- Peak traffic: ~15 GB/s (~120 Gbps)
  <!-- 15M messages/s × 1 KB = 15 GB/s ≈ 120 Gbps -->

## 5. Performance Targets

- Target message delivery latency: <500 ms under normal load; <1 second during peak load
- Target API response latency: <200 ms