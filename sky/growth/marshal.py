import json
import os
import requests
import time

SITE_ROOT = os.path.abspath(os.path.dirname(__file__)+'/../..')

with open(os.path.join(SITE_ROOT, 'config', 'growth.config.json')) as data_file:
    params = json.load(data_file)


class Interface:
    def __init__(self, dbhost_get_url=params['get_id_url'],
                 growth_url=params['growth_url'], instrument_id=65,
                 user=params['growth_user'],
                 passwd=params['growth_pwd']):
        """

        :param dbhost_get_url:
        :param growth_url:
        :param instrument_id:
        :param user:
        :param passwd:
        """

        self.dbhost_url = dbhost_get_url
        self.growth_url = growth_url
        self.instrument_id = instrument_id
        self.user = user
        self.passwd = passwd

    def get_marshal_id_from_dbhost(self, request_id):
        """

        :param request_id:
        :return:
        """
        start = time.time()
        payload = {'request_id': request_id}
        headers = {'content-type': 'application/json'}
        json_data = json.dumps(payload)
        response = requests.post(self.dbhost_url, data=json_data,
                                 headers=headers)
        ret = json.loads(response.text)
        if 'error' in ret:
            return {'elaptime': time.time()-start,
                    'error': 'Error getting the growth id'}
        else:
            return {'elaptime': time.time()-start,
                    'data': ret['marshal_id']}

    def update_growth_status(self, growth_id=None, request_id=None,
                             message="PENDING"):
        """

        :param growth_id:
        :param request_id:
        :param message:
        :return:
        """
        start = time.time()
        if not growth_id and not request_id:
            return {"elaptime": time.time()-start,
                    "error": "No growth id or request id given"}

        if not growth_id and request_id:
            ret = self.get_marshal_id_from_dbhost(request_id)
            if 'error' in ret:
                return ret
            else:
                growth_id = ret['data']

        if not growth_id or not isinstance(growth_id, int):
            return {'elaptime': time.time()-start,
                    'error': growth_id}

        # If we make it to this step then we should have
        # a valid growth marshal target

        status_config = {
            'instrument_id': self.instrument_id,
            'request_id': growth_id,
            'new_status': message
        }

        out_file = open('json_file.txt', 'w')
        out_file.write(json.dumps(status_config))
        out_file.close()

        json_file = open('json_file.txt', 'r')

        ret = requests.post(self.growth_url, auth=(self.user, self.passwd),
                            files={'jsonfile': json_file})

        json_file.close()

        return {'elaptime': time.time()-start,
                'data': ret.status_code}
