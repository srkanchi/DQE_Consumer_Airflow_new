import hvac
from fstvault.FortKnoxAuth import *


class FstVault(object):
    """
    class description
    """

    def __init__(self, url='https://vault.agro.services', aws_auth=False, aws_fort_knox_role=None, aws_arn=None):
        """
        class constructor
        """
        self.root_path = 'secret/fst-apc-engineering-team/'
        # Vault client
        self.client = None
        # Vault server url
        self.url = url
        # assume arn role for testing
        self.assume_role = False
        # Boolean if the application will run on aws
        self.aws_auth = aws_auth
        # If the application is running on aws, a Fort Knox Role needs to be configured
        # Please refer to: https://devtools.monsanto.net/docs/credentials/aws-roles/
        self.aws_fort_knox_role = aws_fort_knox_role
        self.aws_arn = aws_arn
        if self.aws_auth is True and self.aws_fort_knox_role is None:
            raise TypeError('If the aws_auth is enabled, a for knox role needs to be passed. \n \
                            More info: https://devtools.monsanto.net/docs/credentials/aws-roles/')
        # only assume role if arn provided
        if self.aws_arn is not None:
            self.assume_role = True

        # Connects to the client
        self.connect()

    def is_auth(self):
        """
        helper that defines if class is authenticated
        :return: A boolean that defines if the client is authenticated or not
        :rtype: bool
        """
        try:
            return self.client.is_authenticated()
        except Exception as e:
            print(e)
            return False

    def connect(self):
        """
        if using aws will go through fort knox first and get token
        """
        if self.aws_auth:
            rtn = authenticate_fort_knox(self.aws_fort_knox_role, assume_role=self.assume_role, role_arn=self.aws_arn)
            if rtn['status'] == 200:
                the_body = json.loads(rtn["text"])
                self.client = hvac.Client(url=self.url, token=the_body['auth']["client_token"])
                self.client.secrets.kv.default_kv_version = '1'
                if self.client.is_authenticated():
                    return True
            else:
                print(rtn)
                False
        else:
            self.client = hvac.Client(url=self.url)
            self.client.secrets.kv.default_kv_version = '1'
            if self.client.is_authenticated():
                return True

    def list_available_paths(self):
        """
        """
        def get_path(path):
            req = self.client.list(path)
            if req is not None:
                return [path+x for x in req['data']['keys']]
            else:
                return None
        all_paths = []
        def get_paths(list_path):
            new_list = [get_path(x) for x in list_path]
            new_list = [x for x in new_list if x is not None]
            new_list = [item for sublist in new_list for item in sublist]
            all_paths.extend(new_list)
            if len(new_list) > 0:
                get_paths(new_list)
        get_paths([self.root_path])
        all_paths = [x for x in all_paths if x[-1] != '/']
        return all_paths

    def _check_path_api(self, relative_path=None, absolute_path=None):
        """
        """
        if relative_path is None and absolute_path is None:
            raise TypeError("At least one argument need to be passed!")
        if relative_path is not None:
            path = self.root_path + relative_path
        if absolute_path is not None:
            path = absolute_path
        if path.find('secret/') <= 7:
            path = path[7:]
        return path

    def read_secret(self, relative_path=None, absolute_path=None):
        """
        """
        path = self._check_path_api(relative_path, absolute_path)
        try:
            rq = self.client.secrets.kv.read_secret(path)
        except hvac.exceptions.InvalidPath:
            raise ValueError("Invalid Path, please refer to documentation to pass the correct path argument.")
        return rq['data']

    def write_secret(self, data_dict, relative_path=None, absolute_path=None):
        """
        """
        path = self._check_path_api(relative_path, absolute_path)
        try:
            rq =  self.client.secrets.kv.create_or_update_secret(path, secret=data_dict)
        except hvac.exceptions.InvalidPath:
            raise ValueError("Invalid Path, please refer to documentation to pass the correct path argument.")
        return rq

