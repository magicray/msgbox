import json
import time
import requests


class Client:
    def __init__(self, conf_file, channel):
        with open(conf_file) as fd:
            self.servers = {s: 0 for s in json.load(fd)['servers']}
            self.channel = channel

    def tail(self, term, seq):
        while True:
            srv = min([(v, k) for k, v in self.servers.items()])[1]
            url = '{}/{}/{}/{}'.format(srv, self.channel, term, seq)
            res = dict(server=srv, channel=self.channel, term=term, seq=seq)
            try:
                t = time.time()
                r = requests.get(url, allow_redirects=False)
                if 200 != r.status_code:
                    raise Exception('http_response : {}'.format(r.status_code))

                self.servers[srv] = int((time.time() - t)*1000)

                res.update(dict(
                    channel=self.channel, blob=r.content,
                    next_seq=r.headers['x-msgbox-next-seq'],
                    next_term=r.headers['x-msgbox-next-term'],
                    committed_seq=r.headers['x-msgbox-committed-seq']))

                if r.content:
                    yield res

                seq = r.headers['x-msgbox-next-seq']
                term = r.headers['x-msgbox-next-term']
            except requests.exceptions.ConnectionError:
                self.servers[srv] = 10*10
                time.sleep(1)
            except Exception as e:
                self.servers[srv] = 10*10
                res.update(dict(exception=str(e)))
                yield res

    def append(self, blob):
        for i in range(len(self.servers)):
            srv = min([(v, k) for k, v in self.servers.items()])[1]
            url = '{}/{}'.format(srv, self.channel)

            try:
                t = time.time()
                r = requests.post(url, data=blob)
                self.servers[srv] = int((time.time() - t)*1000)

                if 200 == r.status_code:
                    return dict(
                        channel=self.channel,
                        srv=srv,
                        msec=self.servers[srv],
                        seq=r.headers['x-msgbox-seq'],
                        term=r.headers['x-msgbox-term'],
                        length=r.headers['x-msgbox-length'])
            except Exception:
                self.servers[srv] = 10*10
