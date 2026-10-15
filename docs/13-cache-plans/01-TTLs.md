| Cache                  | Suggested TTL       | Reason                                               |
| ---------------------- | ------------------- | ---------------------------------------------------- |
| Authorization: ALLOW   | 30–120 seconds      | Balance DB load and revocation latency               |
| Authorization: DENY_*  | 5–30 seconds        | Avoid repeatedly hitting DB for unauthorized senders |
| Recipients/devices     | 30–120 seconds      | Device changes should propagate reasonably quickly   |
| Session/device routing | Heartbeat-based TTL | Session data is naturally ephemeral                  |

NB: These are rough note