/*
 * SPDX-FileCopyrightText: 2026 HilMail
 * SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-SEL
 */

use common::Server;
use directory::QueryParams;

/// A queued authenticated sender retains only authority still present on its original account.
pub async fn sender_is_authorized(
    server: &Server,
    account_id: u32,
    address: &str,
) -> trc::Result<bool> {
    // The fallback administrator is a platform credential, not a customer domain owner.
    if account_id == u32::MAX {
        return Ok(true);
    }
    Ok(server
        .directory()
        .query(QueryParams::id(account_id).with_return_member_of(false))
        .await?
        .is_some_and(|principal| {
            principal
                .email_addresses()
                .any(|email| email.eq_ignore_ascii_case(address))
        }))
}
