import requests


def _json(result):
    if callable(result.json):
        return result.json()
    else:
        return result.json


class JSONBridgeClient(object):
    src_str = 'json_bridge:nova_db'

    def __init__(self, config):
        self.config = config

    def _url_for_region(self, region):
        return self.config['url'] + self.config['databases'][region]

    def do_query(self, region, query):
        data = {'sql': query}
        credentials = (self.config['username'], self.config['password'])
        return _json(requests.post(self._url_for_region(region), data,
                                   verify=False, auth=credentials))