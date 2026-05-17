//! MQTT adapter: rumqttc subscriber feeding the normalizer.
//!
//! Three layers:
//! - `topic`  — filter-matching + capture-template substitution.
//! - `decode` — payload decoders (json / raw / hex / csv).
//! - `router` — pure (topic, payload, subs) → IngestEvent — heavily
//!   unit-tested.
//! - `subscriber` — the rumqttc-driven async task. Thin wiring; the
//!   reusable logic lives in `router`.

pub mod decode;
pub mod router;
pub mod subscriber;
pub mod topic;

pub use decode::{decode, DecodeError, PayloadFormat};
pub use router::{route, CompileError, CompiledSubscription, RouteError, Subscription};
pub use subscriber::{BrokerConfig, MqttMetrics, MqttSubscriber};
pub use topic::{render, TopicCaptures, TopicMatcher};
