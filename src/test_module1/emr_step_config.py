def add_step_to_emr(task_id, egg, runner):

    add_step = [
        {
            "Name": "Run spark step",
            "ActionOnFailure": "CONTINUE",
            "HadoopJarStep": {
                "Jar": "command-runner.jar",
                "Args": [
                    "spark-submit",
                    "--deploy-mode",
                    "cluster",
                    "--py-files",
                    egg,
                    runner,
                    task_id,
                ],
            },
        }
    ]

    return add_step
