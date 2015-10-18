# Copyright 2015 Quantopian, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you
# may not use this file except in compliance with the License.  You
# may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
# implied.  See the License for the specific language governing
# permissions and limitations under the License.

"""
Redis store for Coal Mine
"""

from coal_mine.abstract_store import AbstractStore

import datetime
import re
from logbook import Logger
import redis


log = Logger('RedisStore')


class RedisCanary:
    def __init__(self, store, id_):
        self.store = store
        self.client = store.client
        self.id = id_
        self.key = '{}:{}'.format(store.namespace, self.id)
        self.emails_k = '{}:emails'.format(self.key)
        self.history_k = '{}:history'.format(self.key)

    def load(self):
        def _load(pipe):
            exists = pipe.exists(self.key)
            if not exists:
                raise KeyError('No such canary {}'.format(self.id))

            pipe.multi()
            pipe.hgetall(self.key)
            pipe.lrange(self.emails_k, 0, -1)
            pipe.zrange(self.history_k, 0, -1, withscores=True)
            pipe.zscore(self.store.deadlines, self.id)

        data, em, hs, deadline = self.client.transaction(_load, self.key)

        data['emails'] = em
        data['history'] = [
            (datetime.datetime.fromtimestamp(float(score)), member)
            for member, score in hs
        ]
        paused = data.get('paused', None)
        if paused is not None:
            paused = bool(int(paused))
        data['paused'] = paused or False

        late = data.get('late', None)
        if late is not None:
            late = bool(int(late))
        data['late'] = late or False

        if 'periodicity' in data:
            try:
                data['periodicity'] = int(data['periodicity'])
            except ValueError:
                try:
                    data['periodicity'] = float(data['periodicity'])
                except ValueError:
                    data['periodicity'] = str(data['periodicity'])

        if not paused:
            data['deadline'] = datetime.datetime.fromtimestamp(
                float(deadline)
            )

        data['id'] = self.id
        return data

    def load_brief(self):
        data = self.client.hmget(self.key, 'name', 'slug')
        if not data:
            raise KeyError('No such canary {}'.format(self.id))

        return {'name': data[0], 'id': self.id, 'slug': data[1]}

    def save(self, canary, create=False):
        data = {}

        def _save(pipe):
            if pipe.exists(self.key):
                if create:
                    raise Exception('slug {} already exists'.format(
                        canary['slug']))

            elif not create:
                raise KeyError('No such canary {}'.format(self.id))

            if create and pipe.hexists(self.store.slugs, canary['slug']):
                raise Exception('id {} already exists'.format(self.id))

            pipe.multi()
            if 'slug' in canary:
                pipe.hset(self.store.slugs, canary['slug'], self.id)
            for key, value in canary.items():
                if value is None:
                    if key not in ('emails', 'history', 'deadline'):
                        pipe.hdel(self.key, key)
                    elif key in ('emails', 'history'):
                        pipe.delete(self.key, key)
                    else:
                        pipe.zrem(self.store.deadlines, self.id)
                else:
                    data[key] = value

            data.pop('id', None)

            paused = data.get('paused', None)
            late = data.get('late', None)

            deadline = data.pop('deadline', None)
            if 'emails' in data and data['emails']:
                pipe.delete(self.emails_k)
                pipe.rpush(self.emails_k, *data.pop('emails'))
            data.pop('emails', None)

            # if data.get('periodicity'):
            if 'history' in data:
                pipe.delete(self.history_k)
                for dt, comment in data.pop('history'):
                    pipe.zadd(self.history_k, dt.timestamp(),
                              comment)
            data.pop('history', None)

            if late:
                pipe.sadd(self.store.late, self.id)
                data['late'] = 1
            elif late is not None:
                data['late'] = 0
                pipe.sadd(self.store.ontime, self.id)

            if paused:
                data['paused'] = 1
                pipe.sadd(self.store.paused, self.id)
                pipe.zrem(self.store.deadlines, self.id)

            elif paused is not None:
                data['paused'] = 0
                pipe.sadd(self.store.active, self.id)
                pipe.zadd(self.store.deadlines,
                          deadline.timestamp(), self.id)

            if data:
                pipe.hmset(self.key, data)

        self.client.transaction(_save, self.key, self.emails_k, self.history_k)

    def create(self, canary):
        self.save(canary, create=True)

    def delete(self):
        def _del(pipe):
            slug = pipe.hget(self.key, 'slug')
            pipe.multi()
            for s in (self.store.active, self.store.paused, self.store.late,
                      self.store.ontime):
                pipe.srem(s, self.id)
            pipe.zrem(self.store.deadlines, self.id)
            pipe.hdel(self.store.slugs, slug)
            pipe.delete(self.history_k)
            pipe.delete(self.emails_k)
            pipe.delete(self.key)

        self.client.transaction(_del, self.key, self.emails_k, self.history_k)


class RedisStore(AbstractStore):

    def __init__(self, *, namespace='cm', **kwargs):
        kwargs['decode_responses'] = True
        self.client = redis.StrictRedis(**kwargs)
        self.namespace = namespace
        self.slugs = self._key('slugs')
        self.active = self._key('active')
        self.paused = self._key('paused')
        self.late = self._key('late')
        self.ontime = self._key('ontime')
        self.deadlines = self._key('deadlines')

    def _key(self, key):
        return '{}:{}'.format(self.namespace, key)

    def create(self, canary):
        cn = RedisCanary(self, canary['id'])
        cn.create(canary)

    def update(self, identifier, updates):
        cn = RedisCanary(self, identifier)
        cn.save(updates)

    def delete(self, identifier):
        cn = RedisCanary(self, identifier)
        cn.delete()

    def get(self, identifier):
        cn = RedisCanary(self, identifier)
        return cn.load()

    def list(self, *, verbose=False, paused=None, late=None, search=None):
        method = 'load_brief'
        if verbose:
            method = 'load'

        sets = []
        if paused is None:
            sets.extend([self.paused, self.active])
        elif paused:
            sets.append(self.paused)
        else:
            sets.append(self.active)

        if late is None:
            sets.extend([self.late, self.ontime])
        elif late:
            sets.append(self.late)
        else:
            sets.append(self.ontime)

        search = re.compile(search) if search is not None else None

        ids = self.client.sunion(*sets)
        for cn in (RedisCanary(self, id) for id in ids):
            res = getattr(cn, method)()
            if search and \
               not search.match(res['name']) and \
               not search.match(res['slug']) and \
               not search.match(res['id']):
                continue

            if method == 'load_brief':
                del res['slug']

            yield res

    def upcoming_deadlines(self):
        for id_ in self.client.zrangebyscore(self.deadlines, '-inf', '+inf'):
            yield RedisCanary(self, id_).load()

    def find_identifier(self, slug):
        id_ = self.client.hget(self.slugs, slug)
        if id_ is None:
            raise KeyError('No such canary {}'.format(slug))
        return id_
