"""
Alerting Module
Sends Slack notifications for ScaleInsights import events.

Pattern follows SP-API/scripts/utils/alerting.py
"""

import os
import json
import logging
import requests
from datetime import datetime
from typing import List, Dict, Optional

logger = logging.getLogger(__name__)


class AlertManager:
    """
    Handles alerting for ScaleInsights import events.

    Usage:
        alert = AlertManager()
        alert.alert_login_failure("Invalid credentials")
        alert.alert_country_failure("US", "Download timeout")
        alert.send_summary(results)
    """

    def __init__(self):
        self.slack_webhook = os.environ.get("SLACK_WEBHOOK_URL")
        self.is_ci = (
            os.environ.get("CI") == "true"
            or os.environ.get("GITHUB_ACTIONS") == "true"
        )

    def _send_slack(self, payload: dict) -> bool:
        """Send message to Slack webhook."""
        if not self.slack_webhook:
            logger.debug("Slack webhook not configured, skipping")
            return False

        try:
            response = requests.post(
                self.slack_webhook,
                json=payload,
                headers={"Content-Type": "application/json"},
                timeout=10,
            )
            if response.status_code == 200:
                logger.debug("Slack notification sent")
                return True
            else:
                logger.warning(f"Slack notification failed: {response.status_code}")
                return False
        except Exception as e:
            logger.warning(f"Slack notification error: {e}")
            return False

    def _github_annotation(self, level: str, message: str):
        """Output GitHub Actions annotation."""
        if self.is_ci:
            print(f"::{level}::{message}")

    def alert_login_failure(self, error: str):
        """Alert when ScaleInsights login fails (critical — aborts entire run)."""
        timestamp = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")

        logger.error(f"SCALEINSIGHTS LOGIN FAILED: {error}")
        self._github_annotation("error", f"ScaleInsights login failed: {error}")

        self._send_slack({
            "attachments": [{
                "color": "#FF0000",
                "blocks": [
                    {
                        "type": "header",
                        "text": {
                            "type": "plain_text",
                            "text": "ScaleInsights Login Failed",
                            "emoji": True,
                        },
                    },
                    {
                        "type": "section",
                        "fields": [
                            {"type": "mrkdwn", "text": f"*Error:*\n{error}"},
                            {"type": "mrkdwn", "text": f"*Impact:*\nAll countries skipped"},
                        ],
                    },
                    {
                        "type": "context",
                        "elements": [
                            {"type": "mrkdwn", "text": f"Time: {timestamp}"},
                        ],
                    },
                ],
            }],
        })

    def alert_country_failure(self, country: str, error: str):
        """Alert when a single country download/import fails."""
        timestamp = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")

        logger.error(f"COUNTRY FAILED: {country} - {error}")
        self._github_annotation("warning", f"ScaleInsights {country} failed: {error}")

        self._send_slack({
            "attachments": [{
                "color": "#FFA500",
                "blocks": [
                    {
                        "type": "header",
                        "text": {
                            "type": "plain_text",
                            "text": f"ScaleInsights {country} Import Failed",
                            "emoji": True,
                        },
                    },
                    {
                        "type": "section",
                        "fields": [
                            {"type": "mrkdwn", "text": f"*Country:*\n{country}"},
                            {"type": "mrkdwn", "text": f"*Error:*\n{error}"},
                        ],
                    },
                    {
                        "type": "context",
                        "elements": [
                            {"type": "mrkdwn", "text": f"Time: {timestamp}"},
                        ],
                    },
                ],
            }],
        })

    def send_summary(
        self,
        results: List[Dict],
        total_keywords: int = 0,
        total_ranks: int = 0,
        duration_seconds: float = 0,
    ):
        """
        Send end-of-run summary.

        Args:
            results: List of dicts with 'country', 'status', 'keywords', 'ranks', 'error'
            total_keywords: Total keywords upserted
            total_ranks: Total rank records upserted
            duration_seconds: Total execution time
        """
        completed = [r for r in results if r.get("status") == "completed"]
        failed = [r for r in results if r.get("status") == "failed"]

        all_success = len(failed) == 0 and len(completed) > 0
        status_text = "All Success" if all_success else f"{len(failed)} Failed"
        timestamp = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")
        duration_str = f"{duration_seconds:.1f}s" if duration_seconds else "N/A"

        logger.info(
            f"IMPORT SUMMARY: {len(completed)}/{len(results)} countries, "
            f"{total_keywords:,} keywords, {total_ranks:,} ranks, {duration_str}"
        )

        # Only send Slack summary if there were failures
        if not all_success:
            country_lines = []
            for r in results:
                emoji = "white_check_mark" if r["status"] == "completed" else "x"
                line = f":{emoji}: {r['country']}"
                if r.get("keywords"):
                    line += f" ({r['keywords']:,} kw, {r.get('ranks', 0):,} ranks)"
                if r.get("error"):
                    line += f" - {r['error']}"
                country_lines.append(line)

            self._send_slack({
                "attachments": [{
                    "color": "#FFA500",
                    "blocks": [
                        {
                            "type": "header",
                            "text": {
                                "type": "plain_text",
                                "text": f"ScaleInsights Import: {status_text}",
                                "emoji": True,
                            },
                        },
                        {
                            "type": "section",
                            "fields": [
                                {
                                    "type": "mrkdwn",
                                    "text": f"*Countries:*\n{len(completed)}/{len(results)} success",
                                },
                                {
                                    "type": "mrkdwn",
                                    "text": f"*Keywords:*\n{total_keywords:,}",
                                },
                                {
                                    "type": "mrkdwn",
                                    "text": f"*Ranks:*\n{total_ranks:,}",
                                },
                                {
                                    "type": "mrkdwn",
                                    "text": f"*Duration:*\n{duration_str}",
                                },
                            ],
                        },
                        {
                            "type": "section",
                            "text": {
                                "type": "mrkdwn",
                                "text": "\n".join(country_lines),
                            },
                        },
                        {
                            "type": "context",
                            "elements": [
                                {"type": "mrkdwn", "text": f"Time: {timestamp}"},
                            ],
                        },
                    ],
                }],
            })
        else:
            logger.info("All countries succeeded — skipping Slack summary")

    def alert_health_check(
        self,
        results: List[Dict],
        fixed_countries: Dict[str, bool],
        still_broken: List[str],
    ):
        """
        Send health check summary to Slack.

        Args:
            results: List of dicts with country, status, today_count, benchmark, deviation
            fixed_countries: Dict of {country: success_bool} for auto-fix attempts
            still_broken: List of country codes still failing after fix
        """
        timestamp = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")
        total = len(results)
        ok_count = sum(1 for r in results if r["status"] == "OK")

        if still_broken:
            color = "#FF0000"
            title = f"ScaleInsights Health Check: {len(still_broken)} FAILED"
        elif fixed_countries:
            color = "#FFA500"
            title = "ScaleInsights Health Check: Auto-Fixed"
        else:
            color = "#36A64F"
            title = "ScaleInsights Health Check: All OK"

        # Build per-country lines
        country_lines = []
        for r in results:
            dev_str = f"{r['deviation']:.1%}" if r.get("deviation") is not None else "N/A"
            bench_str = f"{r['benchmark']:,.0f}" if r.get("benchmark") else "N/A"

            if r["status"] == "OK":
                emoji = "white_check_mark"
                line = f":{emoji}: {r['country']}: {r.get('today_count', 0):,} ranks (median: {bench_str}, {dev_str})"
            elif r["status"] == "MISSING":
                emoji = "x"
                line = f":{emoji}: {r['country']}: MISSING (median: {bench_str})"
            else:
                emoji = "warning"
                line = f":{emoji}: {r['country']}: {r.get('today_count', 0):,} ranks (median: {bench_str}, {dev_str} deviation)"

            # Add fix status if attempted
            if r["country"] in fixed_countries:
                fix_ok = fixed_countries[r["country"]]
                line += " -> re-pulled: " + ("OK" if fix_ok else "FAILED")

            country_lines.append(line)

        logger.info(f"HEALTH CHECK: {ok_count}/{total} OK, "
                     f"{len(fixed_countries)} fixed, {len(still_broken)} broken")

        self._send_slack({
            "attachments": [{
                "color": color,
                "blocks": [
                    {
                        "type": "header",
                        "text": {
                            "type": "plain_text",
                            "text": title,
                            "emoji": True,
                        },
                    },
                    {
                        "type": "section",
                        "fields": [
                            {
                                "type": "mrkdwn",
                                "text": f"*Countries:*\n{ok_count}/{total} OK",
                            },
                            {
                                "type": "mrkdwn",
                                "text": f"*Auto-Fixed:*\n{len(fixed_countries)}",
                            },
                        ],
                    },
                    {
                        "type": "section",
                        "text": {
                            "type": "mrkdwn",
                            "text": "\n".join(country_lines),
                        },
                    },
                    {
                        "type": "context",
                        "elements": [
                            {"type": "mrkdwn", "text": f"Time: {timestamp}"},
                        ],
                    },
                ],
            }],
        })

        # GitHub annotation for failures
        if still_broken:
            self._github_annotation(
                "error",
                f"ScaleInsights health check: {still_broken} still failing"
            )


# Singleton
_alert_manager: Optional[AlertManager] = None


def get_alert_manager() -> AlertManager:
    """Get singleton AlertManager instance."""
    global _alert_manager
    if _alert_manager is None:
        _alert_manager = AlertManager()
    return _alert_manager
