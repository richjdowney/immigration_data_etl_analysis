from airflow.utils.email import send_email


def notify_email(contextDict, **kwargs):
    """Send custom email alerts."""

    # email title.
    title = "Airflow alert: {task_name} Failed".format(**contextDict)

    # email contents
    body = """
    Hi Everyone, <br>
    <br>
    There's been an error in the {task_name} job.<br>
    <br>
    Have fun debugging :),<br>
    Airflow bot <br>
    """.format(
        **contextDict
    )

    send_email("richjdowney@gmail.com", title, body)
