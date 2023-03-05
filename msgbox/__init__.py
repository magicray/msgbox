import random
import requests


class Client:
    def __init__(self, servers):
        self.servers = servers

    def tail(self, seq):
        while True:
            srv = random.choice(self.servers)
            res = dict(server=srv, seq=seq)
            try:
                r = requests.get('{}/{}'.format(srv, seq))
                if 200 != r.status_code:
                    raise Exception('http_response : {}'.format(r.status_code))

                res.update(dict(srv=srv, seq=seq, blob=r.content))
                yield res

                seq += 1
            except Exception as e:
                res.update(dict(exception=str(e)))
                yield res

    def append(self, blob):
        for i in range(len(self.servers)):
            srv = random.choice(self.servers)

            try:
                r = requests.post(srv, data=blob)
                if 200 == r.status_code:
                    return dict(srv=srv,
                                seq=r.headers['x-logdb-seq'],
                                length=r.headers['x-logdb-length'])
            except Exception:
                pass
