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

    def _url_for_region_and_cell(self, region, cell):
        return self.config['url'] + ('prod.%s.%s.nova' % (region, cell))

    def do_query(self, region, query, cell=None):
        data = {'sql': query}
        credentials = (self.config['username'], self.config['password'])
        if cell is not None:
            url = self._url_for_region_and_cell(region, cell)
        else:
            url = self._url_for_region(region)
        return _json(requests.post(url, data,
                                   verify=False, auth=credentials))
