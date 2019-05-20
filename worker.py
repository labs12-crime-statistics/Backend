from decouple import config
import redis
from rq import Worker, Queue, Connection
from multiprocessing import Pool

from utils import get_data

listen = ['high', 'default', 'low']

redis_url = config('REDIS_URL')

CONN = redis.from_url(redis_url)

def start_worker(name):
    with Connection(CONN):
        worker = Worker(map(Queue, listen))
        worker.work()

if __name__ == '__main__':
    with Pool(4) as p:
        p.map(start_worker, ['']*4)
