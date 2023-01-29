import random
import requests


def parse_url(url):
    u = url.split('/')
    if url.startswith('http://') or url.startswith('https://'):
        return '{}//{}'.format(u[0], u[2]), '/'.join(u[3:])
    else:
        return 'http://{}'.format(u[0]), '/'.join(u[1:])


def get_server_list(url):
    r = requests.get(url)
    if 200 == r.status_code:
        return r.json()


def leader(url):
    hostname, path = parse_url(url)
    servers = get_server_list(hostname)

    for i in range(2*len(servers)):
        srv = random.choice(servers)
        r = requests.get('{}/leader/{}'.format(srv, path))
        if 200 == r.status_code:
            r = r.json()
            return dict(leader=r['leader'], term=r['term'], seq=r['seq'])


def tail(url):
    hostname, path = parse_url(url)

    tmp = [t.strip() for t in path.split('/') if t]

    seq = int(tmp[-1])
    term = int(tmp[-2])
    path = '/'.join(tmp[:-2])

    servers = get_server_list(hostname)

    while True:
        srv = random.choice(servers)
        url = '{}/msgbox/{}/{}/{}'.format(srv, path, term, seq)

        try:
            r = requests.get(url, allow_redirects=False)

            if 200 == r.status_code:
                yield dict(status='ok', server=srv, path=path,
                           term=term, seq=seq, blob=r.content)
                seq += 1
            elif 302 == r.status_code:
                seq = 0
                term += 1
            else:
                yield dict(status=r.status_code, server=srv, path=path,
                           term=term, seq=seq)
        except Exception as e:
            yield dict(status='exception', server=srv, path=path,
                       term=term, seq=seq, exeption=str(e))
