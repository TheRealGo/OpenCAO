from __future__ import annotations

import hashlib
import re

_SQL_ALIAS = re.compile(r"[A-Za-z_][A-Za-z0-9_]*\Z")


def codex_delivery_client_id(identity_source: str) -> str:
    """Bind one immutable Delivery identity to the provider's user-message ID."""

    return "cao-delivery-" + hashlib.sha256(identity_source.encode("utf-8")).hexdigest()


def runtime_delivery_lane_head_sql(
    *,
    message_alias: str = "m",
    delivery_alias: str = "d",
) -> str:
    """Return the canonical FIFO-head predicate for one runtime delivery lane.

    Worker commands are ordered by logical recipient and execution runtime.  This
    predicate is shared by dispatcher admission, inbox reads, and acknowledgements
    so none of those surfaces can observe a different lane head.  The aliases are
    source-owned SQL identifiers, never request data.
    """

    if not _SQL_ALIAS.fullmatch(message_alias) or not _SQL_ALIAS.fullmatch(delivery_alias):
        raise ValueError("delivery lane SQL aliases must be static identifiers")
    return f"""
        NOT EXISTS (
            SELECT 1 FROM work_items AS paused_work
            WHERE paused_work.id = {message_alias}.work_item_id
              AND paused_work.paused_boundary_id IS NOT NULL
        )
        AND
        NOT EXISTS (
            SELECT 1
            FROM message_deliveries AS lane_prior
            JOIN messages AS lane_prior_message
              ON lane_prior_message.id = lane_prior.message_id
            WHERE lane_prior.recipient_id = {delivery_alias}.recipient_id
              AND lane_prior_message.sequence < {message_alias}.sequence
              AND lane_prior.state IN (
                  'queued', 'leased', 'dispatched', 'delivered', 'acknowledged'
              )
              AND lane_prior.recipient_attachment_id IS NULL
              AND NOT (
                  lane_prior.state = 'queued'
                  AND lane_prior.owner_token = ''
                  AND EXISTS (
                    SELECT 1 FROM work_items AS paused_prior_work
                    WHERE paused_prior_work.id = lane_prior_message.work_item_id
                      AND paused_prior_work.paused_boundary_id IS NOT NULL
                  )
              )
              AND (
                  (
                    lane_prior.runtime_session_id IS NULL
                    AND {delivery_alias}.runtime_session_id IS NULL
                  )
                  OR lane_prior.runtime_session_id = {delivery_alias}.runtime_session_id
              )
        )
    """


def cao_notification_inbox_visible_sql(
    *,
    delivery_alias: str = "d",
    include_acknowledged: bool = False,
) -> str:
    """Return the visibility predicate for attachment-bound notifications.

    CAO notifications are observations of independently committed Work state,
    not executable Worker commands. Reading one must not require incorporating
    another first. Transport uncertainty remains visible without authorizing
    acknowledgement, replay, or a Work disposition. The caller still binds the
    exact authenticated recipient and attachment and applies cursor pagination.
    The historical include-acknowledged mode also retains handled messages;
    this read-only history option never changes transport eligibility.
    """

    if not _SQL_ALIAS.fullmatch(delivery_alias):
        raise ValueError("delivery lane SQL aliases must be static identifiers")
    if include_acknowledged:
        return f"{delivery_alias}.recipient_attachment_id IS NOT NULL"
    return f"""
        {delivery_alias}.recipient_attachment_id IS NOT NULL
        AND {delivery_alias}.state NOT IN ('acknowledged', 'handled')
    """


def cao_notification_dispatch_head_sql(
    *,
    message_alias: str = "m",
    delivery_alias: str = "d",
) -> str:
    """Serialize notification transport without waiting for CAO reasoning.

    The single parameter is the current UTC timestamp. Queued or leased wakes
    and a live dispatch retain FIFO submission within the exact attachment.
    Confirmed delivery and acknowledgement end that transport fence even when
    the associated decision remains open. An expired, non-executing dispatch
    remains outcome-unknown history; this predicate never requeues it.

    A busy runtime's newest dispatch owner can outlive its transport lease.
    Preserve that execution fence consistently for every Dispatcher instance.
    """

    if not _SQL_ALIAS.fullmatch(message_alias) or not _SQL_ALIAS.fullmatch(delivery_alias):
        raise ValueError("delivery lane SQL aliases must be static identifiers")
    return f"""
        {delivery_alias}.recipient_attachment_id IS NOT NULL
        AND NOT EXISTS (
            SELECT 1
            FROM message_deliveries AS notification_prior
            JOIN messages AS notification_prior_message
              ON notification_prior_message.id = notification_prior.message_id
            WHERE notification_prior.recipient_id = {delivery_alias}.recipient_id
              AND notification_prior.recipient_attachment_id =
                  {delivery_alias}.recipient_attachment_id
              AND notification_prior_message.sequence < {message_alias}.sequence
              AND (
                notification_prior.state IN ('queued', 'leased')
                OR (
                  notification_prior.state = 'dispatched'
                  AND notification_prior.owner_token <> ''
                  AND (
                    notification_prior.lease_until > ?
                    OR EXISTS (
                      SELECT 1
                      FROM runtime_sessions AS notification_runtime
                      WHERE notification_runtime.id = notification_prior.runtime_session_id
                        AND notification_runtime.state = 'busy'
                        AND notification_prior.owner_token = (
                          SELECT newest.owner_token
                          FROM message_deliveries AS newest
                          JOIN messages AS newest_message
                            ON newest_message.id = newest.message_id
                          WHERE newest.runtime_session_id =
                                notification_prior.runtime_session_id
                            AND newest.recipient_id = notification_prior.recipient_id
                            AND newest.recipient_attachment_id =
                                notification_prior.recipient_attachment_id
                            AND newest.state = 'dispatched'
                            AND newest.owner_token <> ''
                          ORDER BY newest_message.sequence DESC
                          LIMIT 1
                        )
                    )
                  )
                )
              )
        )
    """
