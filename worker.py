from decouple import config
import redis
from rq import Worker, Queue, Connection

listen = ['high', 'default', 'low']

redis_url = config('REDIS_URL')

CONN = redis.from_url(redis_url)

if __name__ == '__main__':
    with Connection(CONN):
        worker = Worker(map(Queue, listen))
        worker.work()
