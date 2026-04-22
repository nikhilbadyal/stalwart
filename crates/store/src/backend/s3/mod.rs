/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-SEL
 */

use s3::{Bucket, Region, creds::Credentials};
use std::{fmt::Display, io::Write, ops::Range, time::Duration};
use trc::StoreEvent;
use utils::{
    codec::base32_custom::Base32Writer,
    config::{Config, utils::AsKey},
};

pub struct S3Store {
    bucket: Box<Bucket>,
    prefix: Option<String>,
    max_retries: u32,
}

impl S3Store {
    pub async fn open(config: &mut Config, prefix: impl AsKey) -> Option<Self> {
        // Obtain region and endpoint from config
        let prefix = prefix.as_key();
        let region = config.value_require((&prefix, "region"))?.to_string();
        let region = if let Some(endpoint) = config.value((&prefix, "endpoint")) {
            Region::Custom {
                region: region.to_string(),
                endpoint: endpoint.to_string(),
            }
        } else {
            region.parse().unwrap()
        };
        let credentials = Credentials::new(
            config.value((&prefix, "access-key")),
            config.value((&prefix, "secret-key")),
            config.value((&prefix, "security-token")),
            config.value((&prefix, "session-token")),
            config.value((&prefix, "profile")),
        )
        .map_err(|err| {
            config.new_build_error(
                prefix.as_str(),
                format!("Failed to create credentials: {err:?}"),
            )
        })
        .ok()?;
        let timeout = config
            .property_or_default::<Duration>((&prefix, "timeout"), "30s")
            .unwrap_or_else(|| Duration::from_secs(30));
        /*let allow_invalid = config
        .property_or_default::<bool>((&prefix, "tls.allow-invalid"), "false")
        .unwrap_or_default();*/

        Some(S3Store {
            bucket: Bucket::new(
                config.value_require((&prefix, "bucket"))?,
                region,
                credentials,
            )
            .map_err(|err| {
                config.new_build_error(prefix.as_str(), format!("Failed to create bucket: {err:?}"))
            })
            .ok()?
            .with_path_style()
            /*.set_dangereous_config(allow_invalid, allow_invalid)
            .map_err(|err| {
                config.new_build_error(prefix.as_str(), format!("Failed to create bucket: {err:?}"))
            })
            .ok()?*/
            .with_request_timeout(timeout)
            .map_err(|err| {
                config.new_build_error(prefix.as_str(), format!("Failed to create bucket: {err:?}"))
            })
            .ok()?,
            max_retries: config
                .property_or_default((&prefix, "max-retries"), "3")
                .unwrap_or(3),
            prefix: config.value((&prefix, "key-prefix")).map(|s| s.to_string()),
        })
    }

    pub(crate) async fn get_blob(
        &self,
        key: &[u8],
        range: Range<usize>,
    ) -> trc::Result<Option<Vec<u8>>> {
        let path = self.build_key(key);
        // Remember whether the caller requested a byte range so SignatureDoesNotMatch diagnostics
        // stay limited to the attachment/body-section path instead of changing normal full reads.
        let is_range_read = range.start != 0 || range.end != usize::MAX;
        let mut retries_left = self.max_retries;

        loop {
            let response = if is_range_read {
                self.bucket
                    .get_object_range(
                        &path,
                        range.start as u64,
                        Some(range.end.saturating_sub(1) as u64),
                    )
                    .await
            } else {
                self.bucket.get_object(&path).await
            }
            .map_err(into_error)?;

            match response.status_code() {
                200..=299 => return Ok(Some(response.to_vec())),
                404 => return Ok(None),
                403 if is_range_read && is_signature_mismatch(response.as_slice()) => {
                    // Cloudflare R2 can reject a ranged GET signature even when the same credentials
                    // work for full-object reads, so recover only this known compatibility failure.
                    return self
                        .get_blob_without_range_after_signature_mismatch(&path, range)
                        .await;
                }
                500..=599 if retries_left > 0 => {
                    // wait backoff
                    tokio::time::sleep(Duration::from_secs(
                        1 << (self.max_retries - retries_left).min(6),
                    ))
                    .await;

                    retries_left -= 1;
                }
                code => {
                    return Err(trc::StoreEvent::S3Error
                        .reason(String::from_utf8_lossy(response.as_slice()))
                        .ctx(trc::Key::Code, code));
                }
            }
        }
    }

    async fn get_blob_without_range_after_signature_mismatch(
        &self,
        path: &str,
        range: Range<usize>,
    ) -> trc::Result<Option<Vec<u8>>> {
        // Emit a trace-level diagnostic because this path is a recovered compatibility issue, not
        // an operational S3 failure that should page or pollute warn/error logs.
        trc::event!(
            Store(StoreEvent::BlobRead),
            Details = "Ranged S3 GET returned SignatureDoesNotMatch; using full object read compatibility fallback.",
            Key = path.to_string(),
            Code = 403u16,
        );

        // Retry without the Range header because this uses a different signing path and preserves
        // downloads when an S3-compatible provider rejects the ranged request signature.
        let response = self.bucket.get_object(path).await.map_err(into_error)?;

        match response.status_code() {
            200..=299 => {
                // Slice locally so the caller receives the exact same bytes it requested from the
                // ranged backend path, at the cost of reading the full object only on this failure.
                let bytes = response.as_slice();
                let start = range.start.min(bytes.len());
                let data = if range.end == usize::MAX {
                    bytes.get(start..).unwrap_or_default().to_vec()
                } else {
                    let end = range.end.min(bytes.len());
                    bytes.get(start..end).unwrap_or_default().to_vec()
                };

                // Emit success at trace level so detailed logs can confirm recovery without making
                // normal attachment downloads look like failed S3 operations.
                trc::event!(
                    Store(StoreEvent::BlobRead),
                    Details = "Full S3 GET after ranged SignatureDoesNotMatch succeeded; returned local byte slice.",
                    Key = path.to_string(),
                    Size = data.len(),
                );

                Ok(Some(data))
            }
            404 => Ok(None),
            code => Err(trc::StoreEvent::S3Error
                .reason(String::from_utf8_lossy(response.as_slice()))
                .ctx(trc::Key::Code, code)
                .ctx(
                    trc::Key::Details,
                    "Full S3 GET fallback also failed after ranged SignatureDoesNotMatch.",
                )),
        }
    }

    pub(crate) async fn put_blob(&self, key: &[u8], data: &[u8]) -> trc::Result<()> {
        let mut retries_left = self.max_retries;

        loop {
            let response = self
                .bucket
                .put_object(self.build_key(key), data)
                .await
                .map_err(into_error)?;

            match response.status_code() {
                200..=299 => return Ok(()),
                500..=599 if retries_left > 0 => {
                    // wait backoff
                    tokio::time::sleep(Duration::from_secs(
                        1 << (self.max_retries - retries_left).min(6),
                    ))
                    .await;

                    retries_left -= 1;
                }
                code => {
                    return Err(trc::StoreEvent::S3Error
                        .reason(String::from_utf8_lossy(response.as_slice()))
                        .ctx(trc::Key::Code, code));
                }
            }
        }
    }

    pub(crate) async fn delete_blob(&self, key: &[u8]) -> trc::Result<bool> {
        let mut retries_left = self.max_retries;

        loop {
            let response = self
                .bucket
                .delete_object(self.build_key(key))
                .await
                .map_err(into_error)?;

            match response.status_code() {
                200..=299 => return Ok(true),
                404 => return Ok(false),
                500..=599 if retries_left > 0 => {
                    // wait backoff
                    tokio::time::sleep(Duration::from_secs(
                        1 << (self.max_retries - retries_left).min(6),
                    ))
                    .await;

                    retries_left -= 1;
                }
                code => {
                    return Err(trc::StoreEvent::S3Error
                        .reason(String::from_utf8_lossy(response.as_slice()))
                        .ctx(trc::Key::Code, code));
                }
            }
        }
    }

    fn build_key(&self, key: &[u8]) -> String {
        if let Some(prefix) = &self.prefix {
            let mut writer =
                Base32Writer::with_raw_capacity(prefix.len() + (key.len().div_ceil(4) * 5));
            writer.push_string(prefix);
            writer.write_all(key).unwrap();
            writer.finalize()
        } else {
            Base32Writer::from_bytes(key).finalize()
        }
    }
}

fn is_signature_mismatch(response: &[u8]) -> bool {
    // Match only the provider error code so the fallback does not hide unrelated 403 responses such
    // as AccessDenied or token-scope failures.
    String::from_utf8_lossy(response).contains("<Code>SignatureDoesNotMatch</Code>")
}

#[inline(always)]
fn into_error(err: impl Display) -> trc::Error {
    trc::StoreEvent::S3Error.reason(err)
}
