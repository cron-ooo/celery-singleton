from redis import Redis
from redis.sentinel import Sentinel
from urllib.parse import urlparse

from .base import BaseBackend


class ParseSentinelURL(object):
    default_port = 26379

    def __init__(self, sentinel_url):
        self.urls = [
            url for url in sentinel_url.split(";")
            if url.startswith(r"sentinel://")
        ]

        parsed_urls = [
            urlparse(url)
            for url in self.urls
        ]
        self.sentinels = [
            (instance.hostname, instance.port or self.default_port)
            for instance in parsed_urls
        ]
        self.password = parsed_urls[0].password

class RedisBackend(BaseBackend):
    def __init__(self, *args, **kwargs):
        """
        args and kwargs are forwarded to redis.from_url
        """

        if args[0].startswith(r"sentinel://"):
            sentinel_config = ParseSentinelURL(sentinel_url=args[0])
            broker_transport_options = kwargs["broker_transport_options"]
            sentinel_kwargs = broker_transport_options.get("sentinel_kwargs")

            sentinel = Sentinel(sentinel_config.sentinels,
                                sentinel_kwargs=sentinel_kwargs,
                                password=sentinel_config.password)
            
            self.redis = sentinel.master_for(broker_transport_options["master_name"])

        else:
            self.redis = Redis.from_url(*args, decode_responses=True, **kwargs)

    def lock(self, lock, task_id, expiry=None):
        return not not self.redis.set(lock, task_id, nx=True, ex=expiry)

    def unlock(self, lock):
        self.redis.delete(lock)

    def get(self, lock):
        return self.redis.get(lock)

    def clear(self, key_prefix):
        cursor = 0
        while True:
            cursor, keys = self.redis.scan(cursor=cursor, match=key_prefix + "*")
            for k in keys:
                self.redis.delete(k)
            if cursor == 0:
                break
