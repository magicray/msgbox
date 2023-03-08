import random
import requests

requests.packages.urllib3.disable_warnings()


class Client:
    def __init__(self, servers):
        self.servers = servers

    def tail(self, seq):
        while True:
            srv = 'https://{}'.format(random.choice(self.servers))
            res = dict(server=srv, seq=seq)
            try:
                r = requests.get('{}/{}'.format(srv, seq), verify=False)
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
            srv = 'https://{}'.format(random.choice(self.servers))

            try:
                r = requests.post(srv, data=blob, verify=False)
                if 200 == r.status_code:
                    return dict(srv=srv,
                                seq=r.headers['x-logdb-seq'],
                                length=r.headers['x-logdb-length'])
            except Exception:
                pass

    def put(self, key, value):
        for i in range(len(self.servers)):
            srv = 'https://{}'.format(random.choice(self.servers))

            try:
                r = requests.put('{}/{}'.format(srv, key), data=value,
                                 verify=False)
                if 200 == r.status_code:
                    return dict(srv=srv, status=r.json(),
                                length=r.headers['x-logdb-length'])
            except Exception:
                pass
