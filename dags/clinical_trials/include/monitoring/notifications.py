from airflow.providers.slack.notifications.slack import SlackNotifier
from airflow.utils.context import Context
from airflow.sdk.definitions.connection import AirflowNotFoundException

import logging

def failure_notification(context: Context):
    log = logging.getLogger('airflow.task')
    try:
        ti = context["task_instance"]
        metadata = ti.xcom_pull(task_ids=ti.task_id, key="metadata")

        details = (
            f"FAILURE ALERT: {ti.task_id} for {context['ds']} FAILED\n\n"
            f"Details: {metadata}\n\n"
            f"---------------------------------------------\n\n\n"
        )

        notifier = SlackNotifier(
            slack_conn_id="slack", text=details, channel="dag_alerts"
        )
        notifier.notify(context)

    except AirflowNotFoundException:
        log.warning("Slack connection not configured, skipping notification")
    except Exception as e:
        log.error(f"ERROR raising Slack notification: {str(e)}")

