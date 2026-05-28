**Part 4: The Real-Time Leaderboard**.

---

### **1. The Macro Data Flow**

1. **C++ Artifact Checker** completes 1 second of telemetry analysis.
2. C++ executes `HSET leaderboard_state <team_id> <json_payload>` in Redis.
3. C++ executes `PUBLISH leaderboard_updates <json_payload>` in Redis.
4. **Go Server** receives the Pub/Sub ping.
5. Go Server updates its internal `sync.RWMutex` cache.
6. Go Server broadcasts the JSON to 10,000+ connected **Next.js** clients via WebSockets.


---

### **2. The Data Schema (JSON)**

Every component in this pipeline speaks the exact same JSON language. When C++ creates a payload, it looks like this:

```json
{
  "team_id": "team-abc-123",
  "tps": 45200,
  "p50_latency_ms": 1.2,
  "p90_latency_ms": 4.5,
  "p99_latency_ms": 12.1,
  "is_correct": true,
  "timestamp": 1716540000000
}

```

---

### **3. The Go WebSocket Broadcaster (The Engine)**

This microservice is the powerhouse. It is completely stateless (relying on Redis for recovery) and uses lightweight goroutines to handle massive concurrent traffic.

#### **Core Data Structures**

* **`LeaderboardCache`:** A `sync.RWMutex` map holding the latest JSON string for every `team_id`.
* **`ClientPool`:** A thread-safe registry of all active WebSocket connections.

#### **Endpoints**

* **`GET /api/leaderboard/current`**: Immediately returns the entire `LeaderboardCache` as a JSON array. Used by Next.js to solve the "Cold Start" problem.
* **`GET /stream`**: Upgrades the standard HTTP connection to a WebSocket connection and registers the client in the `ClientPool`.

#### **Internal Functions (Goroutines)**

1. **`InitCache()`**: Runs exactly once on server startup. It executes `HGETALL leaderboard_state` against Redis to instantly populate the `LeaderboardCache` in memory without ever touching TimescaleDB.
2. **`RedisSubscriberLoop()`**: A relentless background goroutine listening to the `leaderboard_updates` channel.
3. **`Broadcast(payload)`**: Triggered by the subscriber loop. It iterates through the `ClientPool` and writes the new JSON payload to every active socket.

---

### **4. The Next.js Frontend (The Presentation)**

Your React frontend is completely dumb to the database. Its only job is to fetch the state, listen to the socket, sort the arrays, and render the UI.

#### **State Management**

You will maintain a single array of objects in your React state:

```javascript
const [leaderboard, setLeaderboard] = useState([]);

```

#### **The Lifecycle Flow (`useEffect`)**

When a judge opens the dashboard, this exact sequence occurs in a single `useEffect` hook:

1. **The Cold Start Fetch:** * Execute `fetch('http://your-go-server/api/leaderboard/current')`.
* Update `setLeaderboard` with the results. (The UI instantly populates).


2. **The Socket Handshake:** * Initialize `const ws = new WebSocket('ws://your-go-server/stream')`.
3. **The Delta Updates (`ws.onmessage`):**
* When a new message arrives, parse the JSON.
* Update the state using a functional state update to prevent stale closures:
```javascript
setLeaderboard((prev) => {
    // Find the team in 'prev' array
    // Overwrite their stats with the new payload
    // Re-sort the array based on TPS or p99
    // Return the new array
});

```




4. **Cleanup:**
* Return `ws.close()` when the component unmounts to prevent memory leaks.



---

### **5. Redis (The Bridge)**

Just to explicitly lock down how C++ interacts with Redis, you are running a pipeline of two commands every second per active team:

| Command | Purpose |
| --- | --- |
| `HSET leaderboard_state <team_id> <json>` | **Persistence.** Creates a permanent snapshot of the current state so the Go server can recover instantly if it crashes. |
| `PUBLISH leaderboard_updates <json>` | **Action.** Instantly kicks the Go server into motion to broadcast the delta update to the browsers. |
