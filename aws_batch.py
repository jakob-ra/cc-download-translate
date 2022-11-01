import boto3
import time

class AWSBatch:
    def __init__(self, n_batches, batch_size, output_bucket, output_path, keywords_path, retry_attempts=3):
        self.n_batches = n_batches
        self.batch_size = batch_size
        self.output_bucket = output_bucket
        self.output_path = output_path
        self.keywords_path = keywords_path
        self.retry_attempts = retry_attempts
        self.batch_client = boto3.client('batch')
        self.batch_env_name = 'cc'

    def create_compute_environment_fargate(self):
        self.batch_client.create_compute_environment(
        computeEnvironmentName=self.batch_env_name,
        type='MANAGED',
        state='ENABLED',
        computeResources={
            'type': 'FARGATE_SPOT',
            'maxvCpus': self.n_batches,
            'subnets': [
                'subnet-3fc5e11e',
                'subnet-96402da7',
                'subnet-84326be2',
                'subnet-3470633a',
                'subnet-789d7434',
                'subnet-d4104a8b',
            ],
            'securityGroupIds': [
                'sg-c4a092d8',
            ],
        },
        tags={
            'Project': 'cc-download'
        },
        # serviceRole='arn:aws:iam::425352751544:role/aws-service-role/batch.amazonaws.com/AWSServiceRoleForBatch',
        )
        time.sleep(5)

    def create_job_queue(self):
        self.batch_client.create_job_queue(
            jobQueueName=self.batch_env_name,
            state='ENABLED',
            priority=1,
            computeEnvironmentOrder=[
                {
                    'order': 1,
                    'computeEnvironment': self.batch_env_name,
                },
            ],
        )
        time.sleep(5)

    def register_job_definition(self):
        self.batch_client.register_job_definition(
            jobDefinitionName=self.batch_env_name,
            type='container',
            containerProperties={
                'image': 'public.ecr.aws/r9v1u7o6/cc-download-translate:latest',
                'resourceRequirements': [
                    {
                        'type': 'VCPU',
                        'value': '0.25',
                    },
                    {
                        'type': 'MEMORY',
                        'value': '512',
                    },
                ],
                'command': [
                    "python3",
                    "./cc-download/cc-download.py",
                    f"--batch_size={self.batch_size}",
                    f"--output_bucket={self.output_bucket}",
                    f"--output_path={self.output_path}",
                    f"--keywords_path={self.keywords_path}",
                ],
                'jobRoleArn': 'arn:aws:iam::425352751544:role/cc-download', #'arn:aws:iam::425352751544:role/ecsTaskExecutionRole',
                'executionRoleArn':  'arn:aws:iam::425352751544:role/cc-download', # 'arn:aws:iam::425352751544:role/ecsTaskExecutionRole',
                'networkConfiguration': {
                        'assignPublicIp': 'ENABLED',
                }
            },
            retryStrategy={
                'attempts': self.retry_attempts,
            },
            timeout={
                'attemptDurationSeconds': 1800
            },
            platformCapabilities=[
                'FARGATE',
            ],
        )
        time.sleep(5)

    def submit_job(self):
        self.batch_client.submit_job(
            jobName=self.batch_env_name,
            jobQueue=self.batch_env_name,
            arrayProperties={
                'size': self.n_batches,
            },
            jobDefinition=self.batch_env_name,
        )
        time.sleep(5)

    def run(self):
        self.create_compute_environment_fargate()
        self.create_job_queue()
        self.register_job_definition()
        self.submit_job()