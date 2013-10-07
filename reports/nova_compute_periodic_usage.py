import datetime
import json
import logging
import os
import sys

sys.path.append(os.environ.get('STACKTACH_INSTALL_DIR', '/stacktach'))

from stacktach import datetime_to_decimal as dt
from stacktach import models
from util.json_bridge_client import JSONBridgeClient

logging.basicConfig()
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

REGIONS = ['dfw']
# REGIONS = ['dfw', 'ord', 'iad', 'lon', 'syd']
CONFIG = {
    "client_class": "JSONBridgeClient",
    "client": {
        "url": "https://mysql-json-bridge.ord.ohthree.com/query/",
        "username": "ozonejenkins",
        "password": "5FJ\"yI0r?6h(r2T",
        "databases": {
            "dfw": "prod.dfw.nova"
        }
    },
    "region_mapping_loc": "/etc/stacktach/region_mapping.json"
}


def get_all_cells(region_name):
    return json_client.get_all_cells(region_name)['result']


def create_report_nova_computes_periodic_usage_not_sent(region_name, cell_name):
    return json_client.get_nova_computes_periodic_usage_not_sent(region_name, cell_name)

def create_report_nova_computes_periodic_usage_errors(region_name, cell_name):
    return json_client.get_nova_computes_periodic_usage_errors(region_name, cell_name)


def get_reports_data():
    reports_data = {}
    for region_name in REGIONS:
        logger.debug(region_name)

        cell_list = get_all_cells(region_name)
        logger.debug(cell_list)
        reports_data[region_name] = {}
        for cell in cell_list:
            cell_name = cell['name']
            logger.debug('')
            logger.debug('    CELL: ' + cell_name)
            reports_data[region_name][cell_name] = {}
            reports_data[region_name][cell_name]['usage_not_sent'] = \
                create_report_nova_computes_periodic_usage_not_sent(
                    region_name, cell_name)
            reports_data[region_name][cell_name]['usage_errors'] = \
                create_report_nova_computes_periodic_usage_errors(
                    region_name, cell_name)
    return reports_data


def __store_report_in_db(start, end, report):
    values = {
        'json': __make_json_report(report),
        'created': dt.dt_to_decimal(datetime.datetime.utcnow()),
        'period_start': start,
        'period_end': end,
        'version': 1,
        'name': 'nova compute periodic usage'
    }

    report = models.JsonReport(**values)
    report.save()


def __make_json_report(report):
    return json.dumps(report)


def __get_previous_period(time):
    last_period = time - datetime.timedelta(days=1)
    start = datetime.datetime(year=last_period.year,
                              month=last_period.month,
                              day=last_period.day)
    end = datetime.datetime(year=time.year,
                            month=time.month,
                            day=time.day)
    return start, end


def create_report(reports_data):
    start, end = __get_previous_period(datetime.datetime.utcnow())
    __store_report_in_db(start, end, reports_data)
    print(reports_data)


def create_stacky_reports():
    reports_data = get_reports_data()
    create_report(reports_data)


class NovaUsageJSONBridgeClient(JSONBridgeClient):
    def get_nova_computes_periodic_usage_not_sent(self, region_name, cell_name):
        logger.debug('    NOVA COMPUTES - PERIODIC USAGE NOT SENT')
        sql = """SELECT services.host AS nova_compute FROM services WHERE services.topic = 'compute' AND services.disabled = 0 AND NOT EXISTS (SELECT * FROM task_log WHERE services.host = task_log.host and task_log.period_beginning = (select max(period_beginning) from task_log)) ORDER BY services.host;"""
        result = self.do_query(region_name, sql, cell=cell_name)['result']
        logger.debug('    RESULT_OF_NOT_SENT: ' + result.__str__())
        return result

    def get_nova_computes_periodic_usage_errors(self, region_name, cell_name):
        logger.debug('    NOVA COMPUTES - PERIODIC USAGE ERRORS')
        sql = """SELECT host AS nova_compute, task_items AS usage_expected, errors AS usage_errors FROM task_log WHERE period_beginning = (SELECT MAX(period_beginning) FROM task_log) AND errors > 0;"""
        result = self.do_query(region_name, sql, cell=cell_name)['result']
        logger.debug('    RESULT_OF_ERROR: ' + result.__str__())
        return result

    def get_all_cells(self, region_name):
        cell_list_sql = "SELECT name FROM cells;"
        return self.do_query(region_name, cell_list_sql)



if __name__ == '__main__':
    json_client = NovaUsageJSONBridgeClient(CONFIG['client'])
    create_stacky_reports()



