/*
 * SPDX-FileCopyrightText: 2026 HilMail
 * SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-SEL
 */

use crate::Store;

/// Holds a cluster-wide delivery barrier without opening a long-running SQL transaction.
pub struct MailDeliveryLease {
    #[cfg(feature = "postgres")]
    client: Option<deadpool_postgres::Client>,
    #[cfg(feature = "postgres")]
    exclusive: bool,
}

impl Store {
    /// Ordinary storage backends remain usable, but cannot advertise distributed release support.
    pub async fn mail_delivery_lease(
        &self,
        exclusive: bool,
    ) -> trc::Result<Option<MailDeliveryLease>> {
        #[cfg(feature = "postgres")]
        if let Self::PostgreSQL(store) = self {
            let client = store
                .barrier_pool
                .get()
                .await
                .map_err(|e| trc::StoreEvent::PostgresqlError.reason(e))?;
            let lease = MailDeliveryLease {
                client: Some(client),
                exclusive,
            };
            // A shared advisory key serializes release against every participating node's active transports.
            lease
                .client
                .as_ref()
                .unwrap()
                .batch_execute(if exclusive {
                    "SELECT pg_advisory_lock(745813)"
                } else {
                    "SELECT pg_advisory_lock_shared(745813)"
                })
                .await
                .map_err(|e| trc::StoreEvent::PostgresqlError.reason(e))?;
            return Ok(Some(lease));
        }
        let _ = exclusive;
        Ok(None)
    }
}

#[cfg(feature = "postgres")]
impl Drop for MailDeliveryLease {
    fn drop(&mut self) {
        if let Some(client) = self.client.take() {
            // Cancellation must close the connection, never recycle a session that might still own a lock.
            let mut guard = DiscardOnDrop(Some(client));
            let exclusive = self.exclusive;
            if let Ok(runtime) = tokio::runtime::Handle::try_current() {
                runtime.spawn(async move {
                    if let Some(client) = &guard.0 {
                        let result = client
                            .batch_execute(if exclusive {
                                "SELECT pg_advisory_unlock(745813)"
                            } else {
                                "SELECT pg_advisory_unlock_shared(745813)"
                            })
                            .await;
                        if result.is_ok() {
                            // Only positively unlocked sessions may return to the reusable pool.
                            guard.0.take();
                        }
                    }
                });
            }
        }
    }
}

#[cfg(feature = "postgres")]
struct DiscardOnDrop(Option<deadpool_postgres::Client>);

#[cfg(feature = "postgres")]
impl Drop for DiscardOnDrop {
    fn drop(&mut self) {
        if let Some(client) = self.0.take() {
            // Taking the object out of the pool drops the actual connection and releases all server-side locks.
            drop(deadpool_postgres::Client::take(client));
        }
    }
}
