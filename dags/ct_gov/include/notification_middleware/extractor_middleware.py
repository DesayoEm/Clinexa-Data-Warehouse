from typing import Dict
from airflow.providers.slack.notifications.slack import SlackNotifier


#This middleware is only meant for tasks within the extractor class as state needs to be within the context of the extractor class
def persist_extraction_state_before_failure(error: Exception, context:Dict, metadata:Dict) -> None:
    """
    Persists extraction state before raising an error in case of failure.
    Metadata is provided by the Extractor class attributes
    Exception is the original exc that was raised during extraction.
    """
    state = {
        "run_state": "FAILED",
        "pages_loaded": metadata.get("pages_loaded"),
        "last_saved_token": metadata.get("last_saved_token"),
        "next_page_url": metadata.get("next_page_url"),
        "date": metadata.get("date"),
    }

    ti = context['task_instance']
    ti.xcom_push(
        key="previous_states",
        value=state
    )

    details = (
        f"CT gov extraction for date: {metadata.get('date')} FAILED\n"
        f"pages loaded: {state['pages_loaded']}\n"
        f"Last saved token: {state['last_saved_token']}\n"
        f"ERROR: {error}"
    )

    # notifier = SlackNotifier(
    #     slack_conn_id='slack',
    #     text=details,
    #     channel='ct-gov'
    # )
    # notifier.notify(context)
    raise error


def persist_extraction_state_before_exit(context:Dict, metadata:Dict) -> None:
    """
    Persists metadata as Xcom In case of a success or early exit or success
    Metadata is provided by the Extractor class attributes
    """
    # tasks are considered successful if extractor loop runs till the end or a page does not have a token to the next one.
    # to be sure the volume 'seems right', metadata will provide no of pages loaded

    #Would I re-run a successful task?
    state = {
        "run_state": "SUCCESS",
        "pages_loaded": metadata.get("pages_loaded"),
        "last_saved_token": metadata.get("last_saved_token"),
        "next_page_url": metadata.get("next_page_url"),
        "date": metadata.get("date"),
    }

    ti = context['task_instance']
    ti.xcom_push(
        key="previous_states",
        value=state
    )

    details = (
        f"CT gov extraction for date: {metadata.get("date")} considered SUCCESSFUL\n"
        f"Pages loaded: {state['pages_loaded']}\n"
        f"Last saved token: {state['last_saved_token']}\n"
    )

    # notifier = SlackNotifier(
    #     slack_conn_id='slack',
    #     text=details,
    #     channel='ct-gov'
    # )
    # notifier.notify(context)



