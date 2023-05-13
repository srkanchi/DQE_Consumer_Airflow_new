#########################
#### Template code for the follwoing process
#### 1. Build a Docker image on EC2
#### 2. Send a Docker image to ECR repo
#### 3. Create an ECS task definition
#### 4. Run a Docker container on ECS
##########################

import base64
import json
import os
import boto3
import docker
from datetime import datetime, timedelta
import sys
import botocore
import os
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
import re


def task_run_processing(ecs_client, task_run_response, maxwait=100):
    """
    """
    failures = task_run_response['failures']
    if len(failures) > 0:
        raise RuntimeError("ERROR on ECS task run submittion")
    print('ECS Task started: %s', task_run_response)

    task_run_arn = task_run_response['tasks'][0]['taskArn']
    waiter = ecs_client.get_waiter('tasks_stopped')
    waiter.config.max_attempts = maxwait
    waiter.wait(
            cluster='fargate',
            tasks=[task_run_arn]

        )
    response = ecs_client.describe_tasks(
            cluster='fargate',
            tasks=[task_run_arn]
        )
    print('ECS checking status: %s', response)
    
    if len(response.get('failures', [])) > 0:
            raise RuntimeError(response)

    for task in response['tasks']:
        # This is a `stoppedReason` that indicates a task has not
        # successfully finished, but there is no other indication of failure
        # in the response.
        # See, https://docs.aws.amazon.com/AmazonECS/latest/developerguide/stopped-task-errors.html # noqa E501
        if re.match(r'Host EC2 \(instance .+?\) (stopped|terminated)\.',
                    task.get('stoppedReason', '')):
            raise RuntimeError(
                'The task was stopped because the host instance terminated: {}'.
                format(task.get('stoppedReason', '')))
        containers = task['containers']
        for container in containers:
            if container.get('lastStatus') == 'STOPPED' and \
                    container['exitCode'] != 0:
                raise RuntimeError(
                    'This task is not in success state {}'.format(task))
            elif container.get('lastStatus') == 'PENDING':
                raise RuntimeError('This task is still pending {}'.format(task))
            elif 'error' in container.get('reason', '').lower():
                raise RuntimeError(
                    'This containers encounter an error during launching : {}'.
                    format(container.get('reason', '').lower()))
    
    print('ECS Task has been successfully executed: %s', response)


###########################
#### define docker process function
#### as python operator
#######################
def docker_process(**kwargs):
	## set up paramters
	print('**** kwargs ****')
	print(kwargs)
	task_id = kwargs['task_id']
	dockerfile_dir = kwargs['dockerfile_dir']
	version_num = kwargs['version_num']
	ecs_task_family = kwargs['ecs_task_family']
	ecs_task_name = kwargs['ecs_task_name']
	containerPort = kwargs['containerPort']
	executionRoleArn = kwargs['executionRoleArn']
	requiresCompatibilities = kwargs['requiresCompatibilities']
	containerCPU = kwargs['containerCPU'] 
	containerMemory = kwargs['containerMemory']
	ecstaskCPU = kwargs['ecstaskCPU']
	ecstaskMemory = kwargs['ecstaskMemory']
	subset1 = kwargs['subset1']
	securityGroups = kwargs['securityGroups']
	assignPublicIp = kwargs['assignPublicIp']

	# print([task_id, dockerfile_dir, version_num, ecs_task_family, ecs_task_name,executionRoleArn, \
	# 		containerPort, requiresCompatibilities, containerCPU, containerMemory,\
	# 		ecstaskCPU, ecstaskMemory, subset1, subset2, securityGroups, assignPublicIp])
	
	LOCAL_REPOSITORY = task_id + ':' + version_num # 'version#'
	LOCAL_REPOSITORY = LOCAL_REPOSITORY.lower()

	#### connect to aws for ecs, ecr
	ecr_client =  boto3.client('ecr')
	ecs_client =  boto3.client('ecs')

	docker_client = docker.from_env()

	## build docker image
	image, build_log = docker_client.images.build(path=dockerfile_dir, tag=LOCAL_REPOSITORY, rm=True)

	## get ecr crdentials
	ecr_credentials = (ecr_client.get_authorization_token()['authorizationData'][0])
	ecr_username = 'AWS'
	ecr_password = (base64.b64decode(ecr_credentials['authorizationToken']).replace(b'AWS:', b'').decode('utf-8'))

	## get ecr url
	ecr_url = ecr_credentials['proxyEndpoint']
	print("***ecr_url****")
	print(ecr_url)

	## use docker client to login/authenticate with ECR
	docker_client.login(username=ecr_username, password=ecr_password, registry=ecr_url)

	## get ECR repo name
	ecr_repo_name = '{}/{}'.format(ecr_url.replace('https://', ''), LOCAL_REPOSITORY)
	print('****ecr_repo_name*** ')
	print(ecr_repo_name)

	try:
		r1 = ecr_client.list_images(repositoryName=LOCAL_REPOSITORY.split(':')[0])
		print('*** ECR repo already exisits ***')
		print(r1)
	except:
		r2 = ecr_client.create_repository(repositoryName=LOCAL_REPOSITORY.split(':')[0])
		print('*** Create a new repo on ECR ***')
		print(r2)

	## tag docker image
	tag_flag = image.tag(ecr_repo_name, tag=version_num)
	print('**** Image tagging flag ****')
	print(tag_flag)

	# #### test for task definition
	# task_def_list = ecs_client.list_task_definitions(familyPrefix=ecs_task_family)
	# print('*** Print task_def_list ****')
	# print(task_def_list)

	## push image to AWS ECR
	push_log = docker_client.images.push(ecr_repo_name, tag=version_num)
	print('*** Image push log ****')
	print(push_log)

	## register ecs task
	ecs_response = ecs_client.register_task_definition(
	    family=ecs_task_family,
	    containerDefinitions=[
	        {
	            "name": ecs_task_name,
	            "image": ecr_repo_name,
	            "cpu": containerCPU,
	            "memory": containerMemory, 
	            "essential": True,
	            "portMappings": [
	            	{
					"containerPort": containerPort, 
					"hostPort": containerPort,
					"protocol": 'tcp'
					}
	            ],
				'logConfiguration': {
					"logDriver": "awslogs",
					"options": {
						"awslogs-group": "airflow-tasks",
						"awslogs-region": "us-east-1",
						"awslogs-stream-prefix": ecs_task_name
					}
				}
	        }
	    ],
	    networkMode="awsvpc",
		executionRoleArn= executionRoleArn, 
		taskRoleArn=executionRoleArn, 
		requiresCompatibilities=[
			requiresCompatibilities # This should be "FARGATE"
		],
		cpu=ecstaskCPU,
		memory=ecstaskMemory
		#### tags can be put here
	    # tags=[ 
	    #     {"key": "createdBy", "value": "moto-unittest"},
	    #     {"key": "foo", "value": "bar"},
	    # ],
	)
	print("ECS RESPONSE: ")
	print(ecs_response)


	# ## check task definition
	# task_def_list = ecs_client.list_task_definitions(familyPrefix=ecs_task_family)
	# print('*** Check: task_def_list ****')
	# print(task_def_list)


	## run task
	run_task_response = ecs_client.run_task(
		cluster='fargate', 
		launchType='FARGATE',
		taskDefinition=ecs_task_name,  # <-- notice no revision number
		count=1,
		platformVersion='LATEST',
		networkConfiguration={
	        'awsvpcConfiguration': {
	            'subnets': [
	                subset1
	            ],
	            'securityGroups': [
	                securityGroups
	            ],
	            'assignPublicIp': assignPublicIp 
	        }
	    },
		)
	print("**** Finished running ecs task *****")
	print(run_task_response)
	print("**** Finished pushing ecs task *****")
	task_run_processing(ecs_client, run_task_response, maxwait=100)
	print("**** Finished running ecs task *****")

	return None


###########################
#### default args of dag
###########################
default_args = {
		'owner'                 : 'Airflow',
		'description'           : 'Use of the DockerOperator',
		'depend_on_past'        : False,
		'start_date'            : datetime(2020, 1, 1),
		'email_on_failure'      : False,
		'email_on_retry'        : False,
		'retries'               : 1,
		'retry_delay'           : timedelta(minutes=1)
		}

###########################
#### define default args of python function
###########################

## DAG name: used in airflow
mainDir = 'DQEConsumer_airflow'
NAME = mainDir.lower()
dagName = NAME

## dag for redshift query
DIR_NAME = mainDir
task_id = NAME

default_docker_args = {
		'task_id'				: NAME, 
		'dockerfile_dir'		: '/home/ubuntu/airflow/dags/AirflowDAGs/{}/'.format(DIR_NAME),
		'version_num'			: 'version1', 
		'ecs_task_name'			: NAME,
		'ecs_task_family'		: NAME, 
		'containerPort'			: 9999,
		'requiresCompatibilities': 'FARGATE',
		'containerCPU'			: 1024, 
		'containerMemory'		: 400,
		'ecstaskCPU'			: "2048",
		'ecstaskMemory'			: "4096",
		'executionRoleArn'		: 'arn:aws:iam::517496737367:role/ecsTaskExecutionRole',
		'subset1'				: 'subnet-073079799b9a9e249',
		'securityGroups'		: 'sg-085d6b5c00415aa36',
		'assignPublicIp'		: 'ENABLED'
		}

## DAG name: used in airflow
dagName = NAME

###########################
#### Assign task defintion as DAG and Run DAG
###########################

with DAG(dagName, default_args=default_args, schedule_interval="0 */5 * * *", catchup=False) as dag:

	## bash operator: print date
	t1 = BashOperator(
	task_id='print_current_date',
	bash_command='date'
	)


	## python operator: docker process: build, push, run docker image on ECR, ECS
	t2 = PythonOperator(task_id=task_id, \
		python_callable=docker_process, \
		op_kwargs = default_docker_args
		)

	## bash operator: print ending
	t3 = BashOperator(
	task_id='print_end',
	bash_command='echo "**** End or process ****"'
	)


	t1 >> t2 >> t3
