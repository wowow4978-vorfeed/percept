//! Live MQTT subscriber. Drives a rumqttc `EventLoop` and feeds matched
//! messages through the pure `route` function into the normalizer's
//! `IngestEnvelope` channel — same envelope shape as HTTP ingest, so the
//! rest of the pipeline doesn't care.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use rumqttc::{AsyncClient, Event, EventLoop, MqttOptions, Packet, QoS};
use tokio::sync::mpsc;

use crate::metrics::Metrics;
use crate::normalizer::IngestEnvelope;

use super::router::{route, CompiledSubscription};

#[derive(Debug, Clone)]
pub struct BrokerConfig {
    pub id: String,
    pub host: String,
    pub port: u16,
    pub client_id: String,
    pub username: Option<String>,
    pub password: Option<String>,
    pub keep_alive: Duration,
}

#[derive(Default)]
pub struct MqttMetrics {
    pub messages_received: AtomicU64,
    pub messages_unmatched: AtomicU64,
    pub messages_decode_failed: AtomicU64,
    pub reconnects: AtomicU64,
}

impl MqttMetrics {
    pub fn render_into(&self, broker_id: &str, out: &mut String) {
        use std::fmt::Write;
        let _ = writeln!(
            out,
            "# HELP percept_mqtt_messages_total Messages received per broker."
        );
        let _ = writeln!(out, "# TYPE percept_mqtt_messages_total counter");
        let _ = writeln!(
            out,
            "percept_mqtt_messages_total{{broker=\"{broker_id}\"}} {}",
            self.messages_received.load(Ordering::Relaxed)
        );
        let _ = writeln!(
            out,
            "# HELP percept_mqtt_unmatched_total Messages that matched no [[mqtt.subscription]]."
        );
        let _ = writeln!(out, "# TYPE percept_mqtt_unmatched_total counter");
        let _ = writeln!(
            out,
            "percept_mqtt_unmatched_total{{broker=\"{broker_id}\"}} {}",
            self.messages_unmatched.load(Ordering::Relaxed)
        );
        let _ = writeln!(
            out,
            "# HELP percept_mqtt_decode_failed_total Messages dropped because the payload decoder failed."
        );
        let _ = writeln!(out, "# TYPE percept_mqtt_decode_failed_total counter");
        let _ = writeln!(
            out,
            "percept_mqtt_decode_failed_total{{broker=\"{broker_id}\"}} {}",
            self.messages_decode_failed.load(Ordering::Relaxed)
        );
        let _ = writeln!(
            out,
            "# HELP percept_mqtt_reconnects_total MQTT EventLoop reconnects."
        );
        let _ = writeln!(out, "# TYPE percept_mqtt_reconnects_total counter");
        let _ = writeln!(
            out,
            "percept_mqtt_reconnects_total{{broker=\"{broker_id}\"}} {}",
            self.reconnects.load(Ordering::Relaxed)
        );
    }
}

pub struct MqttSubscriber {
    broker: BrokerConfig,
    subscriptions: Vec<CompiledSubscription>,
    tx: mpsc::Sender<IngestEnvelope>,
    metrics: Arc<MqttMetrics>,
    ingest_metrics: Arc<Metrics>,
}

impl MqttSubscriber {
    pub fn new(
        broker: BrokerConfig,
        subscriptions: Vec<CompiledSubscription>,
        tx: mpsc::Sender<IngestEnvelope>,
        metrics: Arc<MqttMetrics>,
        ingest_metrics: Arc<Metrics>,
    ) -> Self {
        Self {
            broker,
            subscriptions,
            tx,
            metrics,
            ingest_metrics,
        }
    }

    /// Connect to the broker, subscribe to every configured topic filter,
    /// and forward matched messages onto the normalizer channel. Runs
    /// until the channel is closed or the EventLoop fails terminally.
    pub async fn run(self) {
        let mut options =
            MqttOptions::new(&self.broker.client_id, &self.broker.host, self.broker.port);
        options.set_keep_alive(self.broker.keep_alive);
        if let (Some(u), Some(p)) = (&self.broker.username, &self.broker.password) {
            options.set_credentials(u, p);
        }
        let (client, eventloop) = AsyncClient::new(options, 32);

        for sub in &self.subscriptions {
            if let Err(e) = client.subscribe(&sub.topic_filter, QoS::AtLeastOnce).await {
                tracing::error!(broker = %self.broker.id, filter = %sub.topic_filter, err = %e, "mqtt subscribe failed");
            }
        }
        self.drive_event_loop(eventloop).await;
    }

    async fn drive_event_loop(self, mut eventloop: EventLoop) {
        let broker_id = self.broker.id.clone();
        loop {
            match eventloop.poll().await {
                Ok(Event::Incoming(Packet::Publish(p))) => {
                    self.metrics
                        .messages_received
                        .fetch_add(1, Ordering::Relaxed);
                    handle_publish(
                        &p.topic,
                        &p.payload,
                        &self.subscriptions,
                        &self.tx,
                        &self.metrics,
                        &self.ingest_metrics,
                        &broker_id,
                    )
                    .await;
                }
                Ok(_) => {}
                Err(e) => {
                    self.metrics.reconnects.fetch_add(1, Ordering::Relaxed);
                    tracing::warn!(broker = %broker_id, err = %e, "mqtt event-loop error; rumqttc will reconnect");
                    tokio::time::sleep(Duration::from_secs(1)).await;
                }
            }
        }
    }
}

async fn handle_publish(
    topic: &str,
    payload: &[u8],
    subscriptions: &[CompiledSubscription],
    tx: &mpsc::Sender<IngestEnvelope>,
    mqtt_metrics: &MqttMetrics,
    ingest_metrics: &Metrics,
    broker_id: &str,
) {
    let now_ms = percept_core::now_ms_utc();
    let result = route(subscriptions, topic, payload, now_ms, Some(broker_id));
    match result {
        Ok(event) => {
            let envelope = IngestEnvelope {
                event,
                token_name: Some(format!("mqtt:{broker_id}")),
            };
            if let Err(err) = tx.try_send(envelope) {
                match err {
                    mpsc::error::TrySendError::Full(_) => {
                        ingest_metrics.inc_consumer_drop("mqtt_subscriber");
                    }
                    mpsc::error::TrySendError::Closed(_) => {
                        tracing::warn!(broker = %broker_id, "ingest channel closed; mqtt subscriber done");
                    }
                }
            }
        }
        Err(super::router::RouteError::NoMatch(_)) => {
            mqtt_metrics
                .messages_unmatched
                .fetch_add(1, Ordering::Relaxed);
        }
        Err(
            super::router::RouteError::Decode(_) | super::router::RouteError::UnresolvedKind(_),
        ) => {
            mqtt_metrics
                .messages_decode_failed
                .fetch_add(1, Ordering::Relaxed);
        }
        Err(super::router::RouteError::Template(e)) => {
            tracing::error!(broker = %broker_id, err = %e, topic = %topic, "mqtt template render failed");
            mqtt_metrics
                .messages_decode_failed
                .fetch_add(1, Ordering::Relaxed);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mqtt::{decode::PayloadFormat, router::Subscription};

    fn compiled(filter: &str, template: &str, kind: &str) -> CompiledSubscription {
        CompiledSubscription::compile(Subscription {
            topic_filter: filter.into(),
            source_id_template: template.into(),
            kind: Some(kind.into()),
            kind_field: None,
            payload: PayloadFormat::Json,
        })
        .unwrap()
    }

    #[tokio::test]
    async fn matched_publish_pushes_envelope() {
        let (tx, mut rx) = mpsc::channel(8);
        let mqtt_metrics = MqttMetrics::default();
        let ingest = Metrics::new();
        handle_publish(
            "home/kitchen/temp",
            b"{\"c\":20}",
            &[compiled("home/+/temp", "temp.{+1}", "temperature")],
            &tx,
            &mqtt_metrics,
            &ingest,
            "broker",
        )
        .await;
        let env = rx.try_recv().unwrap();
        assert_eq!(env.event.source_id, "temp.kitchen");
        assert_eq!(env.event.kind, "temperature");
    }

    #[tokio::test]
    async fn unmatched_publish_increments_counter() {
        let (tx, _rx) = mpsc::channel(8);
        let mqtt_metrics = MqttMetrics::default();
        let ingest = Metrics::new();
        handle_publish(
            "weather/today",
            b"{}",
            &[compiled("home/+/temp", "temp.{+1}", "k")],
            &tx,
            &mqtt_metrics,
            &ingest,
            "broker",
        )
        .await;
        assert_eq!(mqtt_metrics.messages_unmatched.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn decode_failed_increments_counter() {
        let (tx, _rx) = mpsc::channel(8);
        let mqtt_metrics = MqttMetrics::default();
        let ingest = Metrics::new();
        handle_publish(
            "home/kitchen/temp",
            b"{ not json",
            &[compiled("home/+/temp", "temp.{+1}", "k")],
            &tx,
            &mqtt_metrics,
            &ingest,
            "broker",
        )
        .await;
        assert_eq!(
            mqtt_metrics.messages_decode_failed.load(Ordering::Relaxed),
            1
        );
    }

    #[tokio::test]
    async fn bus_full_increments_consumer_drop() {
        // Capacity-1 channel pre-filled => the next send hits Full.
        let (tx, _rx) = mpsc::channel(1);
        let prefill = CompiledSubscription::compile(Subscription {
            topic_filter: "z".into(),
            source_id_template: "z".into(),
            kind: Some("z".into()),
            kind_field: None,
            payload: PayloadFormat::Json,
        })
        .unwrap();
        let env = IngestEnvelope {
            event: route(&[prefill], "z", b"{}", 0, None).unwrap(),
            token_name: None,
        };
        tx.try_send(env).unwrap();

        let mqtt_metrics = MqttMetrics::default();
        let ingest = Metrics::new();
        handle_publish(
            "home/kitchen/temp",
            b"{}",
            &[compiled("home/+/temp", "temp.{+1}", "k")],
            &tx,
            &mqtt_metrics,
            &ingest,
            "broker",
        )
        .await;
        assert_eq!(ingest.consumer_drop_count("mqtt_subscriber"), 1);
    }
}
