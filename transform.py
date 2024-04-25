import boto3


def lambda_handler(event, context):
    client = boto3.client('emr', region_name='us-east-1')

    # Especificar la VPC y las subredes
    subnet_id = 'subnet-096a9732a4fde351f'

    cluster_id = client.run_job_flow(
        Name='EMR-Spark',
        ReleaseLabel='emr-6.2.0',
        Applications=[{'Name': 'Spark'}],
        Configurations=[{
            'Classification': 'spark-env',
            'Properties': {},
            'Configurations': [{
                'Classification': 'export',
                'Properties': {'PYSPARK_PYTHON': '/usr/bin/python3'}
            }]
        }],
        Instances={
            'InstanceGroups': [
                {
                    'Name': "Master nodes",
                    'Market': 'ON_DEMAND',
                    'InstanceRole': 'MASTER',
                    'InstanceType': 'm5.xlarge',
                    'InstanceCount': 1,
                },
                {
                    'Name': "Worker nodes",
                    'Market': 'ON_DEMAND',
                    'InstanceRole': 'CORE',
                    'InstanceType': 'm5.xlarge',
                    'InstanceCount': 2,
                }
            ],
            'Ec2KeyName': 'vockey',
            'KeepJobFlowAliveWhenNoSteps': False,
            'TerminationProtected': False,
            'Ec2SubnetId': subnet_id,
        },
        Steps=[{
            'Name': 'Run Spark script',
            'ActionOnFailure': 'TERMINATE_CLUSTER',
            'HadoopJarStep': {
                'Jar': 'command-runner.jar',
                'Args': ["spark-submit",
                         "--deploy-mode",
                         "cluster",
                         "s3://zappa-transform-parcial2/script/script.py"]
            }
        }],
        JobFlowRole='EMR_EC2_DefaultRole',
        ServiceRole='EMR_Notebooks_DefaultRole',
        VisibleToAllUsers=True
    )

    print(f"Started cluster {cluster_id}")
    return {
        'statusCode': 200,
        'body': f"Cluster {cluster_id} started successfully"
    }
