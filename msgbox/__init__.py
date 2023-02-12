import json
import time
import random
import requests


class Client:
    def __init__(self, conf_file):
        with open(conf_file) as fd:
            self.servers = json.load(fd)['servers']

    def tail(self, seq):
        while True:
            srv = random.choice(self.servers)
            res = dict(server=srv, seq=seq)
            try:
                t = time.time()
                r = requests.get('{}/{}'.format(srv, seq))
                if 200 != r.status_code:
                    raise Exception('http_response : {}'.format(r.status_code))

                msec = int((time.time() - t)*1000)

                res.update(dict(srv=srv, seq=seq, msec=msec, blob=r.content))
                yield res

                seq += 1
            except Exception as e:
                res.update(dict(exception=str(e)))
                yield res

    def append(self, blob):
        for i in range(len(self.servers)):
            srv = random.choice(self.servers)

            try:
                t = time.time()
                r = requests.post(srv, data=blob)
                msec = int((time.time() - t)*1000)

                if 200 == r.status_code:
                    return dict(
                        srv=srv,
                        msec=msec,
                        seq=r.headers['x-msgbox-seq'],
                        length=r.headers['x-msgbox-length'])
            except Exception:
                pass
