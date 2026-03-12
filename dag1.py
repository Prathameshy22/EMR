from airflow import DAG
from airflow.providers.amazon.aws.operators.emr import EmrAddStepsOperator
from airflow.providers.amazon.aws.sensors.emr import EmrStepSensor
from datetime import datetime

CLUSTER_ID = "j-2X9E8XGAIA3NK"

SPARK_STEPS = [
    {
        'Name': 'S3 to S3 Move',
        'ActionOnFailure': 'CONTINUE',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': [
                'spark-submit',
                's3://emr-source-bucket-prj2/code/spark.py'
            ]
        }
    }
]

with DAG(
    dag_id="s3_to_s3_emr_pipeline",
    start_date=datetime(2026,3,11),
    schedule_interval=None,
    catchup=False
) as dag:

    add_step = EmrAddStepsOperator(
        task_id='add_emr_step',
        job_flow_id=CLUSTER_ID,
        steps=SPARK_STEPS,
        aws_conn_id='aws_default'
    )

    watch_step = EmrStepSensor(
        task_id='watch_step',
        job_flow_id=CLUSTER_ID,
        step_id="{{ task_instance.xcom_pull(task_ids='add_emr_step')[0] }}",
        aws_conn_id='aws_default'
    )

    add_step >> watch_step