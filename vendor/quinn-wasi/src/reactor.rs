//! Bridges wasi pollables to tokio wakers.
//!
//! wasi readiness is a pollable resource with no callback; tokio's
//! current-thread parker knows nothing of pollables. The bridge is a task:
//! sockets register their pollables here and arm them with wakers, and the
//! reactor sweeps armed pollables with the non-blocking `ready()`, waking
//! every waker whose pollable reports ready. Between sweeps it sleeps one
//! millisecond through `tokio::time`, which keeps the runtime parked in
//! tokio's own timer driver — tokio timers fire on schedule, and socket
//! readiness is seen at most a millisecond late. With nothing armed the
//! reactor waits on a notification and costs nothing.
//!
//! wasi pollables are level-triggered, so a sweep can never lose an event:
//! a datagram that arrives between sweeps keeps the pollable ready until
//! the socket drains it.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::task::Waker;
use std::time::Duration;

use wasi::io::poll::Pollable;

const SWEEP: Duration = Duration::from_millis(1);

/// Whether the reactor waits for an armed socket inside `wasi:io/poll`
/// rather than sleeping between sweeps; see [`park_in_poll`].
static PARK_IN_POLL: AtomicBool = AtomicBool::new(false);

/// Has the reactor, with sockets armed and none of them ready, wait in
/// `wasi:io/poll.poll` on those sockets and a clock of one sweep, after
/// letting every other runnable task go first, instead of sleeping a
/// sweep through `tokio::time`. A datagram is then seen the moment the
/// host sees it rather than at the next sweep, which matters where the
/// host's timer is coarse: under wasmtime on Windows a one millisecond
/// sleep lasts about 15.6 ms. The cost is that a tokio timer due within
/// that sweep fires when the wait ends, at most one sweep late, where
/// before the runtime parked until it exactly. Off by default.
pub fn park_in_poll(on: bool) {
    PARK_IN_POLL.store(on, Ordering::Relaxed);
}

/// Names one registered pollable.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Key(usize);

struct Entry {
    pollable: Pollable,
    // Armed while non-empty; one waker per waiting task.
    wakers: Vec<Waker>,
}

#[derive(Default)]
struct State {
    next: usize,
    entries: HashMap<usize, Entry>,
}

/// The pollable-to-waker bridge. One per runtime, shared by every socket.
pub struct Reactor {
    state: Mutex<State>,
    armed: tokio::sync::Notify,
}

impl Reactor {
    /// Creates the reactor and spawns its sweep task onto the ambient
    /// tokio runtime.
    pub fn spawn() -> Arc<Self> {
        let reactor = Arc::new(Self {
            state: Mutex::new(State::default()),
            armed: tokio::sync::Notify::new(),
        });
        tokio::spawn(Arc::clone(&reactor).run());
        reactor
    }

    /// The process-wide reactor, spawned on first use.
    ///
    /// The spawn binds it to the runtime current at that moment; a guest
    /// runs exactly one runtime, so the binding holds for the process.
    pub fn global() -> Arc<Self> {
        static GLOBAL: OnceLock<Arc<Reactor>> = OnceLock::new();
        Arc::clone(GLOBAL.get_or_init(Reactor::spawn))
    }

    /// Takes ownership of `pollable` and returns its key.
    pub fn register(&self, pollable: Pollable) -> Key {
        let mut state = self.state.lock().unwrap();
        let key = state.next;
        state.next += 1;
        state.entries.insert(
            key,
            Entry {
                pollable,
                wakers: Vec::new(),
            },
        );
        Key(key)
    }

    /// Arms `key`: `waker` is woken once the pollable reports ready.
    pub fn arm(&self, key: Key, waker: &Waker) {
        let mut state = self.state.lock().unwrap();
        let entry = state.entries.get_mut(&key.0).expect("armed a removed key");
        if !entry.wakers.iter().any(|w| w.will_wake(waker)) {
            entry.wakers.push(waker.clone());
        }
        drop(state);
        // notify_one stores a permit, so an arm just before the reactor
        // waits is never lost.
        self.armed.notify_one();
    }

    /// Removes `key`, dropping its pollable and any pending wakers.
    pub fn remove(&self, key: Key) {
        self.state.lock().unwrap().entries.remove(&key.0);
    }

    async fn run(self: Arc<Self>) {
        loop {
            let mut woken = Vec::new();
            let mut any_armed = false;
            {
                let mut state = self.state.lock().unwrap();
                for entry in state.entries.values_mut() {
                    if entry.wakers.is_empty() {
                        continue;
                    }
                    if entry.pollable.ready() {
                        woken.append(&mut entry.wakers);
                    } else {
                        any_armed = true;
                    }
                }
            }
            let progressed = !woken.is_empty();
            for waker in woken {
                waker.wake();
            }
            if progressed {
                // Let the woken tasks run (and likely re-arm) before the
                // next sweep.
                tokio::task::yield_now().await;
            } else if any_armed && PARK_IN_POLL.load(Ordering::Relaxed) {
                // Every other runnable task, and tokio's expired timers,
                // first: the wait below blocks the thread.
                tokio::task::yield_now().await;
                self.wait(SWEEP);
            } else if any_armed {
                tokio::time::sleep(SWEEP).await;
            } else {
                self.armed.notified().await;
            }
        }
    }
}

impl Reactor {
    /// Blocks until an armed pollable is ready or `longest` has passed.
    fn wait(&self, longest: Duration) {
        let state = self.state.lock().unwrap();
        let clock = wasi::clocks::monotonic_clock::subscribe_duration(
            u64::try_from(longest.as_nanos()).unwrap_or(u64::MAX),
        );
        let mut waiting: Vec<&Pollable> = state
            .entries
            .values()
            .filter(|entry| !entry.wakers.is_empty())
            .map(|entry| &entry.pollable)
            .collect();
        waiting.push(&clock);
        let _ = wasi::io::poll::poll(&waiting);
    }
}

impl std::fmt::Debug for Reactor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let state = self.state.lock().unwrap();
        f.debug_struct("Reactor")
            .field("entries", &state.entries.len())
            .finish()
    }
}
