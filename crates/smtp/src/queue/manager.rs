/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-SEL
 */

use super::{Message, QueueId, Status, spool::SmtpSpool};
use crate::queue::{Recipient, spool::LOCK_EXPIRY};
use ahash::AHashMap;
use common::{
    Inner,
    config::smtp::queue::{QueueExpiry, QueueName},
    core::BuildServer,
    ipc::{QueueEvent, QueueEventStatus},
};
use rand::{Rng, seq::SliceRandom};
use std::{
    collections::hash_map::Entry,
    sync::{Arc, atomic::Ordering},
    time::{Duration, Instant},
};
use store::write::now;
use tokio::sync::mpsc;

pub struct Queue {
    pub core: Arc<Inner>,
    pub locked_messages: LockedMessages,
    pub stats: AHashMap<QueueName, QueueStats>,
    pub next_wake_up: Instant,
    pub rx: mpsc::Receiver<QueueEvent>,
}

#[derive(Debug)]
pub struct QueueStats {
    pub in_flight: usize,
    pub max_in_flight: usize,
    pub last_warning: Instant,
}

#[derive(Debug)]
pub struct LockedMessages {
    pub locked: AHashMap<(QueueId, QueueName), LockedMessage>,
    pub revision: u64,
}

#[derive(Debug)]
pub struct LockedMessage {
    pub expires: u64,
    pub revision: u64,
}

impl SpawnQueue for mpsc::Receiver<QueueEvent> {
    fn spawn(self, core: Arc<Inner>) {
        tokio::spawn(async move {
            Queue::new(core, self).start().await;
        });
    }
}

const BACK_PRESSURE_WARN_INTERVAL: Duration = Duration::from_secs(60);

impl Queue {
    pub fn new(core: Arc<Inner>, rx: mpsc::Receiver<QueueEvent>) -> Self {
        Queue {
            core,
            locked_messages: LockedMessages::default(),
            stats: AHashMap::new(),
            next_wake_up: Instant::now(),
            rx,
        }
    }

    pub async fn start(&mut self) {
        let mut is_paused = false;

        loop {
            let refresh_queue = match tokio::time::timeout(
                self.next_wake_up.duration_since(Instant::now()),
                self.rx.recv(),
            )
            .await
            {
                Ok(Some(QueueEvent::WorkerDone {
                    queue_id,
                    queue_name,
                    status,
                })) => {
                    let queue_stats = self.stats.get_mut(&queue_name).unwrap();
                    queue_stats.in_flight -= 1;

                    match status {
                        QueueEventStatus::Completed => {
                            self.locked_messages.locked.remove(&(queue_id, queue_name));
                            !self.locked_messages.locked.is_empty() || !queue_stats.has_capacity()
                        }
                        QueueEventStatus::Locked => {
                            let expires = LOCK_EXPIRY + rand::rng().random_range(5..10);
                            let due_in = Instant::now() + Duration::from_secs(expires);
                            if due_in < self.next_wake_up {
                                self.next_wake_up = due_in;
                            }

                            self.locked_messages.locked.insert(
                                (queue_id, queue_name),
                                LockedMessage {
                                    expires: now() + expires,
                                    revision: self.locked_messages.revision,
                                },
                            );
                            self.locked_messages.locked.len() > 1 || !queue_stats.has_capacity()
                        }
                        QueueEventStatus::Deferred => {
                            self.locked_messages.locked.remove(&(queue_id, queue_name));
                            true
                        }
                    }
                }
                Ok(Some(QueueEvent::Refresh)) => true,
                Ok(Some(QueueEvent::Paused(paused))) => {
                    self.core
                        .data
                        .queue_status
                        .store(!paused, Ordering::Relaxed);
                    is_paused = paused;
                    false
                }
                Ok(Some(QueueEvent::ReloadSettings)) => {
                    let server = self.core.build_server();
                    for (name, settings) in &server.core.smtp.queue.virtual_queues {
                        if let Some(stats) = self.stats.get_mut(name) {
                            stats.max_in_flight = settings.threads;
                        } else {
                            self.stats.insert(*name, QueueStats::new(settings.threads));
                        }
                    }

                    false
                }
                Err(_) => true,
                Ok(Some(QueueEvent::Stop)) | Ok(None) => {
                    break;
                }
            };

            if !is_paused {
                // Deliver scheduled messages
                if refresh_queue || self.next_wake_up <= Instant::now() {
                    // Process queue events
                    let server = self.core.build_server();
                    let mut queue_events = server.next_event(self).await;

                    if queue_events.messages.len() > 3 {
                        queue_events.messages.shuffle(&mut rand::rng());
                    }

                    for queue_event in &queue_events.messages {
                        // Fetch queue stats
                        let stats = match self.stats.get_mut(&queue_event.queue_name) {
                            Some(stats) => stats,
                            None => {
                                let queue_config =
                                    server.get_virtual_queue_or_default(&queue_event.queue_name);
                                self.stats.insert(
                                    queue_event.queue_name,
                                    QueueStats::new(queue_config.threads),
                                );
                                self.stats.get_mut(&queue_event.queue_name).unwrap()
                            }
                        };

                        // Enforce concurrency limits
                        if stats.has_capacity() {
                            // Deliver message
                            stats.in_flight += 1;
                            queue_event.try_deliver(server.clone());
                        } else {
                            if stats.last_warning.elapsed() >= BACK_PRESSURE_WARN_INTERVAL {
                                stats.last_warning = Instant::now();
                                trc::event!(
                                    Queue(trc::QueueEvent::BackPressure),
                                    Reason = "Processing capacity for this queue exceeded.",
                                    QueueName = queue_event.queue_name.to_string(),
                                    Limit = stats.max_in_flight,
                                );
                            }
                            self.locked_messages
                                .locked
                                .remove(&(queue_event.queue_id, queue_event.queue_name));
                        }
                    }

                    // Remove expired locks
                    let now = now();
                    self.locked_messages.locked.retain(|_, locked| {
                        locked.expires > now && locked.revision == self.locked_messages.revision
                    });

                    self.next_wake_up = Instant::now()
                        + Duration::from_secs(queue_events.next_refresh.saturating_sub(now));
                }
            } else {
                // Queue is paused
                self.next_wake_up = Instant::now() + Duration::from_secs(86400);
            }
        }
    }
}

impl Message {
    pub fn next_event(&self, queue: Option<QueueName>) -> Option<u64> {
        let mut next_event = None;

        for rcpt in &self.recipients {
            if matches!(rcpt.status, Status::Scheduled | Status::TemporaryFailure(_))
                && queue.is_none_or(|q| rcpt.queue == q)
            {
                let mut earlier_event = std::cmp::min(rcpt.retry.due, rcpt.notify.due);

                if let Some(expires) = rcpt.expiration_time(self.created) {
                    earlier_event = std::cmp::min(earlier_event, expires);
                }

                if let Some(next_event) = &mut next_event {
                    if earlier_event < *next_event {
                        *next_event = earlier_event;
                    }
                } else {
                    next_event = Some(earlier_event);
                }
            }
        }

        next_event
    }

    pub fn next_delivery_event(&self, queue: Option<QueueName>) -> Option<u64> {
        let mut next_delivery = None;

        for rcpt in self.recipients.iter().filter(|rcpt| {
            matches!(rcpt.status, Status::Scheduled | Status::TemporaryFailure(_))
                && queue.is_none_or(|q| rcpt.queue == q)
        }) {
            if let Some(next_delivery) = &mut next_delivery {
                if rcpt.retry.due < *next_delivery {
                    *next_delivery = rcpt.retry.due;
                }
            } else {
                next_delivery = Some(rcpt.retry.due);
            }
        }

        next_delivery
    }

    pub fn next_dsn(&self, queue: Option<QueueName>) -> Option<u64> {
        let mut next_dsn = None;

        for rcpt in self.recipients.iter().filter(|rcpt| {
            matches!(rcpt.status, Status::Scheduled | Status::TemporaryFailure(_))
                && queue.is_none_or(|q| rcpt.queue == q)
        }) {
            if let Some(next_dsn) = &mut next_dsn {
                if rcpt.notify.due < *next_dsn {
                    *next_dsn = rcpt.notify.due;
                }
            } else {
                next_dsn = Some(rcpt.notify.due);
            }
        }

        next_dsn
    }

    pub fn expires(&self, queue: Option<QueueName>) -> Option<u64> {
        let mut expires = None;

        for rcpt in self.recipients.iter().filter(|d| {
            matches!(d.status, Status::Scheduled | Status::TemporaryFailure(_))
                && queue.is_none_or(|q| d.queue == q)
        }) {
            if let Some(rcpt_expires) = rcpt.expiration_time(self.created) {
                if let Some(expires) = &mut expires {
                    if rcpt_expires > *expires {
                        *expires = rcpt_expires;
                    }
                } else {
                    expires = Some(rcpt_expires)
                }
            }
        }

        expires
    }

    pub fn next_events(&self) -> AHashMap<QueueName, u64> {
        let mut next_events = AHashMap::new();

        for rcpt in &self.recipients {
            if matches!(rcpt.status, Status::Scheduled | Status::TemporaryFailure(_)) {
                let mut earlier_event = std::cmp::min(rcpt.retry.due, rcpt.notify.due);

                if let Some(expires) = rcpt.expiration_time(self.created) {
                    earlier_event = std::cmp::min(earlier_event, expires);
                }

                match next_events.entry(rcpt.queue) {
                    Entry::Occupied(mut entry) => {
                        let entry = entry.get_mut();
                        if earlier_event < *entry {
                            *entry = earlier_event;
                        }
                    }
                    Entry::Vacant(entry) => {
                        entry.insert(earlier_event);
                    }
                }
            }
        }

        next_events
    }
}

impl Recipient {
    pub fn expiration_time(&self, created: u64) -> Option<u64> {
        match self.expires {
            QueueExpiry::Duration(time) => Some(created + time),
            QueueExpiry::Count(_) => None,
        }
    }

    pub fn is_expired(&self, created: u64, now: u64) -> bool {
        match self.expires {
            QueueExpiry::Duration(time) => created + time <= now,
            QueueExpiry::Count(count) => self.retry.inner >= count,
        }
    }
}

pub trait SpawnQueue {
    fn spawn(self, core: Arc<Inner>);
}

impl QueueStats {
    fn new(max_in_flight: usize) -> Self {
        QueueStats {
            in_flight: 0,
            max_in_flight,
            last_warning: Instant::now() - BACK_PRESSURE_WARN_INTERVAL,
        }
    }

    #[inline]
    pub fn has_capacity(&self) -> bool {
        self.in_flight < self.max_in_flight
    }
}

impl Default for LockedMessages {
    fn default() -> Self {
        LockedMessages {
            locked: AHashMap::with_capacity(128),
            revision: 0,
        }
    }
}
