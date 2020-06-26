def add_step_to_emr():

    test_step = [
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
                    "s3://immigration-data-etl/test_submit-0.1-py3.7.egg",
                    "s3://immigration-data-etl/spark_runner.py",
                    "module 1",
                ],
            },
        }
    ]

    return test_step
