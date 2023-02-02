import json
import random
import requests


class Client:
    def __init__(self, conf_file):
        with open(conf_file) as fd:
            self.conf = json.load(fd)

    def tail(self, path, term, seq):
        while True:
            srv = random.choice(self.conf['servers'])
            url = '{}/{}/{}/{}'.format(srv, path, term, seq)
            res = dict(server=srv, path=path, term=term, seq=seq)
            try:
                r = requests.get(url, allow_redirects=False)

                res.update(dict(
                    path=path, blob=r.content,
                    next_seq=r.headers['x-msgbox-next-seq'],
                    next_term=r.headers['x-msgbox-next-term'],
                    committed_seq=r.headers['x-msgbox-committed-seq']))

                if r.content:
                    yield res

                seq = r.headers['x-msgbox-next-seq']
                term = r.headers['x-msgbox-next-term']
            except Exception as e:
                res.update(dict(exception=str(type(e))))
                yield res

    def get(self, path):
        for srv in self.conf['servers']:
            url = '{}/{}'.format(srv, path)

            try:
                r = requests.get(url)
                if 200 == r.status_code:
                    return dict(
                        path=path, blob=r.content,
                        seq=r.headers['x-msgbox-seq'],
                        term=r.headers['x-msgbox-term'],
                        next_seq=r.headers['x-msgbox-next-seq'],
                        next_term=r.headers['x-msgbox-next-term'],
                        committed_seq=r.headers['x-msgbox-committed-seq'])
            except Exception:
                pass

        return 'Service Unavailable'

    def append(self, path, blob):
        for srv in self.conf['servers']:
            url = '{}/{}'.format(srv, path)

            try:
                r = requests.post(url, data=blob)
                if 200 == r.status_code:
                    return dict(
                        path=path,
                        seq=r.headers['x-msgbox-seq'],
                        term=r.headers['x-msgbox-term'],
                        length=r.headers['x-msgbox-length'])
            except Exception:
                pass

        return 'Service Unavailable'
