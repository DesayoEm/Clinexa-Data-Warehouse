from typing import Dict

def persist_before_failure(error: Exception, context:Dict, metadata:Dict) -> None:
    info = {
        "task": context['task_instance'].task_id,
        "date": metadata.get("date"),
    }

    for key, value in metadata.items():
        info[key] = value

    ti = context['task_instance']
    ti.xcom_push(
        key=f"{info['task']} results",
        value=info
    )

    extra_info = {k: v for k, v in info.items() if k not in ['task', 'date']}

    details = (
        f"{info['task']} for CT gov extraction for {info['date']} FAILED\n"
        f"info: {extra_info}\n\n"
        f"ERROR: {error}"
    )

    # notifier = SlackNotifier(
    #     slack_conn_id='slack',
    #     text=details,
    #     channel='ct-gov'
    # )
    # notifier.notify(context)
    raise error


def persist_before_exit(context:Dict, metadata:Dict) -> None:
    info = {
        "task": context['task_instance'].task_id,
        "date": metadata.get("date"),
    }
    for key, value in metadata.items():
        info[key] = value

    ti = context['task_instance']
    ti.xcom_push(
        key=f"{info['task']} results",
        value=info
    )

    extra_info = {k: v for k, v in info.items() if k not in ['task', 'date']}

    details = (
        f"{info['task']} for CT gov extraction for {info['date']} FAILED\n"
        f"info: {extra_info}\n\n"

    )

    # notifier = SlackNotifier(
    #     slack_conn_id='slack',
    #     text=details,
    #     channel='ct-gov'
    # )
    # notifier.notify(context)



