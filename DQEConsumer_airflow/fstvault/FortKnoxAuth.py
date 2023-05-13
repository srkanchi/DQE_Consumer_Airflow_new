import base64
import json
import boto3
import requests


def headers_to_go_style(headers):
    """
    re-format the headers to the required format
    """
    reformatted = {}
    for k, v in headers.items():
        if isinstance(v, bytes):
            reformatted[k] = [str(v, 'ascii')]
        else:
            reformatted[k] = [v]
    return reformatted


def assume_role_session(client_in, arn):
    """
    assume role - used for testing on ec2 to override default credentials
    returns new assumed session
    """

    # assume role for testing
    assumed_role_object=client_in.assume_role(
        RoleArn=arn,
        RoleSessionName="AssumeRoleSession1"
    )

    # From the response that contains the assumed role, get the temporary
    # credentials that can be used to make subsequent API calls
    credentials=assumed_role_object['Credentials']

    # restart
    session = boto3.session.Session(
    aws_access_key_id=credentials['AccessKeyId'],
    aws_secret_access_key=credentials['SecretAccessKey'],
    aws_session_token=credentials['SessionToken']
    )

    return session


def generate_vault_request(role_name, client):
    """
    get necessary IAM details for vault request
    """

    endpoint = client._endpoint
    operation_model = client._service_model.operation_model('GetCallerIdentity')
    request_dict = client._convert_to_request_dict({}, operation_model)
    request = endpoint.create_request(request_dict, operation_model)
    # It's now signed...
    return {
        'iam_http_request_method': request.method,
        'iam_request_url': str(base64.b64encode(request.url.encode('ascii')), 'ascii'),
        'iam_request_body': str(base64.b64encode(request.body.encode('ascii')), 'ascii'),
        'iam_request_headers': str(base64.b64encode(bytes(json.dumps(headers_to_go_style(dict(request.headers))), 'ascii')), 'ascii'),  # It's a CaseInsensitiveDict, which is not JSON-serializable
        'role': role_name,
    }


def authenticate_fort_knox(role_name, assume_role=False, role_arn=None):
    """
    post vault auth request and return

    token can be accessed by
    dict_of_text = json.loads(rtn["text"])
    token=dict_of_text['auth']["client_token"])
    """

    session = boto3.session.Session()
    # if you have credentials from non-default sources, call
    # session.set_credentials here, before calling session.create_client
    client = session.client('sts')

    if assume_role:
        session = assume_role_session(client, role_arn)
        client = session.client('sts')

    body = generate_vault_request(role_name, client)
    r = requests.post("https://vault.agro.services/v1/auth/aws/login", json=body)
    return {'status': r.status_code,
            'text' : r.text}
