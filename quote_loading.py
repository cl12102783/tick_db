from influxdb import InfluxDBClient
import datetime
import urllib3, shutil
import pandas as pd
import numpy as np
from multiprocessing import Pool
import warnings
warnings.filterwarnings("ignore")

influx_client = InfluxDBClient('10.10.2.8', 8086, 'root', password='root''mydb')
influx_client.switch_database('BitMex_Liquidation_test')


def load_quote(results):
    body = {}
    body['measurement'] = 'quote'
    body['tags'] = {'symbol': results['symbol']}
    body['time'] = results['timestamp'].replace('D', 'T')

    del results['symbol'], results['timestamp']

    body['fields'] = results

    for member in list(body['fields'].keys()):  # list is to avoid iteration runtime error caused by removing keys
        if pd.isnull(body['fields'][member]):
            del body['fields'][member]
        else:
            body['fields'][member] = float(body['fields'][member])

    if len(body['fields']) != 0:
        influx_client.write_points([body])

    return

if __name__ == '__main__':
    start = datetime.datetime.strptime("20141122", "%Y%m%d")
    end = datetime.datetime.strptime("20191023", "%Y%m%d")  # the end date is not included, so +1
    date_generated = [start + datetime.timedelta(days=x) for x in range(0, (end - start).days)]
    pool = Pool()  # multiprocessing
    for date in np.flip(date_generated):
        date = date.strftime("%Y%m%d")
        url = 'https://s3-eu-west-1.amazonaws.com/public.bitmex.com/data/quote/' + str(date) + '.csv.gz'
        c = urllib3.PoolManager()
        filename = "day_quote.csv.gz"
        with c.request('Get', url, preload_content=False) as res, open(filename, 'wb') as out_file:
            shutil.copyfileobj(res, out_file)
        # Do it in multiprocessing
        record = pd.read_csv('day_quote.csv.gz').to_dict(orient='record')
        pool.map(load_quote, [chunk for chunk in record])
