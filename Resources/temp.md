## The mental model first

Your bot fleet has **4 layers of concepts**. Learn them in this order — each layer builds on the previous one.

```
Layer 1 — C++ OOP         →  bot_payload.hpp, rest_bot.hpp/cpp
Layer 2 — C++ Concurrency →  thread_worker.hpp/cpp
Layer 3 — Networking I/O  →  fleet_commander.hpp/cpp, telemetry.hpp/cpp
Layer 4 — Build & Deploy  →  CMakeLists.txt, Dockerfile, main.cpp
```

---

## Layer 1 — `bot_payload.hpp` + `rest_bot.hpp/cpp`

These two files are pure C++ OOP. No networking, no threads. Understand these first.

**Concepts: virtual destructor, pure virtual function, inheritance, polymorphism**

- **YouTube (20 min):** `https://www.youtube.com/watch?v=77eueMbWI0Y` — covers inheritance + virtual functions + abstract base class with code examples. Directly maps to `BotPayload` and `RestBot`.

- **Article (5 min read):** `https://www.geeksforgeeks.org/cpp/pure-virtual-functions-and-abstract-classes/` — short, has code snippets. Read the section on why `virtual ~BotPayload() = default` is non-optional — memory leak without it.

**Concept: `std::mt19937` random number generator (used in `rest_bot.cpp`)**

- **Article (5 min):** `https://www.learncpp.com/cpp-tutorial/generating-random-numbers-using-mersenne-twister/` — explains mt19937, seeding, and `std::uniform_int_distribution` with clear examples. Critically covers why each thread needs its own instance — a shared generator across threads causes contention. This is exactly why every `RestBot` has its own `rng_`.

---

## Layer 2 — `thread_worker.hpp/cpp`

This is the hardest file. Break it into three sub-concepts.

**Sub-concept A: What is `io_uring` and why does it exist**

- **Article (10 min):** `https://unixism.net/loti/what_is_io_uring.html` — explains io_uring as a new async I/O API for Linux created to address limitations of `select`, `poll`, `epoll`, and `aio`. Read this before touching Boost.Asio — you need to understand what's happening at the kernel level.

- **Article (8 min):** `https://developers.redhat.com/articles/2023/04/12/why-you-should-use-iouring-network-io` — specifically covers io_uring for network I/O, which is your exact use case — it explains where the wins are and where they're modest.

**Sub-concept B: C++20 coroutines and `co_await`**

- **YouTube playlist (watch videos 1 and 5 only):** `https://www.youtube.com/playlist?list=PLqCJpWy5FohfFg4GugueCdIHKjcB1zW3O` — covers async processing, coroutines, and networking with Boost.Asio end to end. Video 1 sets up context. Video 5 is `co_await` — the exact pattern in `run_bot()` and `send_orders()`.

- **Production talk (30 min, watch after the playlist):** `https://www.youtube.com/watch?v=RBldGKfLb9I` — covers C++20 coroutines with Boost.Asio in production — why coroutines replace callback hell and how they work in real systems.

**Sub-concept C: CPU pinning and shared-nothing architecture**

This is the `pthread_setaffinity_np` call in `thread_worker.cpp`. No dedicated video needed — just read this one short article: search **"CPU pinning Linux C++ pthread_setaffinity_np"** on GeeksforGeeks. The concept is: Thread 0 stays on core 0 forever, Thread 1 stays on core 1. No OS migration mid-benchmark = stable latency measurements.

---

## Layer 3 — `fleet_commander.hpp/cpp` + `telemetry.hpp/cpp`

**Concept: Boost.Beast HTTP server (used in `fleet_commander.cpp`)**

- **YouTube (15 min):** `https://www.youtube.com/watch?v=eXnIIVZ2z2s` — Boost.Asio + Beast for HTTP. FleetCommander is exactly this: an async HTTP server that accepts a POST and fires off work.

**Concept: `std::mutex` + `std::lock_guard` (used in `telemetry.cpp`)**

- **Article (5 min):** `https://www.geeksforgeeks.org/cpp/mutex-in-cpp/` — the telemetry flush uses a mutex only on the 500ms flush path (not the hot path), this explains why that's acceptable.

**Concept: `std::array` as a lock-free counter (used in `telemetry.hpp`)**

No video needed. The key insight is just this: `counts[HTTP_200]++` on a private `std::array` is lock-free because **no other thread touches it**. The enum maps events to indices — O(1) with zero hashing. Just re-read `telemetry.hpp` with this in mind and it'll click.

---

## Layer 4 — `CMakeLists.txt` + `Dockerfile` + `main.cpp`

**CMake:**
- **YouTube (20 min):** `https://www.youtube.com/watch?v=nlKcXZVpfqA` — "CMake Tutorial for Beginners" — covers `add_executable`, `target_link_libraries`, `find_library`. Directly maps to your `CMakeLists.txt`.

**Multi-stage Dockerfile:**
- **Article (8 min):** `https://docs.docker.com/build/building/multi-stage/` — official Docker docs on multi-stage builds. Your Dockerfile has a `builder` stage (compiles C++) and a `runtime` stage (minimal image). This explains exactly why — smaller final image, no build tools in production.

**`main.cpp` — `getenv()` for config:**
- Nothing to learn here. It's 20 lines. `std::getenv` reads environment variables — same as Python's `os.getenv`. Read it once and you're done.

---

## Exact order to read all 12 files

```
Day 1  →  bot_payload.hpp → rest_bot.hpp → rest_bot.cpp
           (OOP video + mt19937 article)

Day 2  →  telemetry.hpp → telemetry.cpp
           (mutex article, then re-read the flush logic)

Day 3  →  thread_worker.hpp → thread_worker.cpp
           (io_uring article → Asio playlist videos 1+5 → production talk)

Day 4  →  fleet_commander.hpp → fleet_commander.cpp
           (Boost.Beast video)

Day 5  →  CMakeLists.txt → Dockerfile → main.cpp
           (CMake video + Docker multi-stage article)
```