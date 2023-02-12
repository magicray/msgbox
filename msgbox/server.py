import os
import time
import json
import uuid
import sanic
import signal
import random
import asyncio
import aiohttp
import argparse


APP = sanic.Sanic('MsgBox')
signal.alarm(random.randint(1, 10))


class G:
    max_seq = None
    start_time = time.strftime('%y%m%d-%H%M%S')


# All files and directory should be in the assigned location
def abspath(*args):
    args = [str(a) for a in args]
    return os.path.join(os.getcwd(), 'data', *args)


# Form a hierarchical path to avoid too many files in a directory
def seq2path(seq):
    batch_size = 10**2

    filename = seq % batch_size

    one = int(seq / batch_size) % batch_size
    two = int(seq / batch_size**2) % batch_size
    three = int(seq / batch_size**3) % batch_size

    return abspath(str(three), str(two), str(one), str(filename))


# Atomic file creation. Write a tmp file and then move to final path
def blob_dump(path, blob):
    os.makedirs(os.path.dirname(path), exist_ok=True)

    tmppath = '{}-{}.tmp'.format(path, uuid.uuid4())

    with open(tmppath, 'wb') as fd:
        fd.write(blob)

    os.rename(tmppath, path)


@APP.post('/<seq:int>')
async def write(request, seq):
    if not allowed(request):
        raise sanic.exception.Unauthorized()

    if seq > G.max_seq:
        G.max_seq = seq

    filepath = seq2path(int(seq))

    if not os.path.isfile(filepath):
        blob_dump('{}.blob'.format(filepath), request.body)
        blob_dump(filepath, json.dumps(dict(
            promised_seq=1, accepted_seq=1,
            accepted_val=True), indent=4).encode())

        return sanic.response.json(dict(status='OK'))

    raise sanic.exception.Forbidden()


@APP.post('/')
async def append(request):
    n = len(ARGS.servers)
    seq = int((G.max_seq+n)/n)*n + ARGS.index
    G.max_seq = seq

    res = await Client.multi_post('{}'.format(seq), request.body)
    ok = [True for r in res.values() if r['status'] == 'OK']

    if len(ok) >= ARGS.quorum:
        return sanic.response.json(seq, headers={
            'x-msgbox-seq': seq,
            'x-msgbox-length': len(request.body)})

    raise sanic.exceptions.ServiceUnavailable()


@APP.post('/paxos/<phase:str>/<seq:int>')
def paxos_server(request, phase, seq):
    if not allowed(request):
        raise sanic.exception.Unauthorized()

    paxos = dict(promised_seq=0, accepted_seq=0, accepted_val=None)

    filepath = seq2path(seq)
    if os.path.isfile(filepath):
        with open(filepath) as fd:
            paxos = json.loads(fd.read())

    if 'learned_val' in paxos:
        return sanic.response.json(
            dict(status='LEARNED', learned_val=paxos['learned_val']))

    request = request.json

    if 'promise' == phase and request['proposal_seq'] > paxos['promised_seq']:
        blob_dump(filepath, json.dumps(dict(
            promised_seq=request['proposal_seq'],
            accepted_seq=paxos['accepted_seq'],
            accepted_val=paxos['accepted_val']), indent=4).encode())

        return sanic.response.json(dict(
            status='OK',
            accepted_seq=paxos['accepted_seq'],
            accepted_val=paxos['accepted_val']))

    if 'accept' == phase and request['proposal_seq'] >= paxos['promised_seq']:
        blob_dump(filepath, json.dumps(dict(
            promised_seq=request['proposal_seq'],
            accepted_seq=request['proposal_seq'],
            accepted_val=request['proposal_val']), indent=4).encode())

        return sanic.response.json(dict(status='OK'))

    if 'learn' == phase and request['proposal_seq'] >= paxos['promised_seq']:
        blob_dump(filepath, json.dumps(dict(
            promised_seq=request['proposal_seq'],
            accepted_seq=request['proposal_seq'],
            accepted_val=request['proposal_val'],
            learned_val=request['proposal_val']), indent=4).encode())

        return sanic.response.json(dict(status='OK'))

    return sanic.response.json(dict(status=None))


@APP.get('/blob/<seq:int>')
async def blob_read(request, seq):
    filepath = '{}.blob'.format(seq2path(seq))

    if os.path.isfile(filepath):
        with open(filepath, 'rb') as fd:
            return sanic.response.raw(fd.read())

    return sanic.response.text('NOT_FOUND', status=404)


@APP.get('/<seq:int>')
async def paxos_client(request, seq):
    seq = int(seq)

    if seq > G.max_seq:
        await asyncio.sleep(1)
        raise sanic.exceptions.NotFound()

    paxos = dict()
    paxospath = seq2path(seq)
    if os.path.isfile(paxospath):
        with open(paxospath) as fd:
            paxos = json.load(fd)

    if 'learned_val' not in paxos or paxos['learned_val'] is True:
        # Fetch file from other nodes if this node does not have yet
        blobpath = '{}.blob'.format(paxospath)
        for srv in ARGS.servers:
            if not os.path.isfile(blobpath):
                res = await Client.get_blob('{}/blob/{}'.format(srv, seq))
                if 200 == res['status']:
                    blob_dump(blobpath, res['blob'])

    if 'learned_val' not in paxos:
        paxos_seq = int(time.time()*10**6)
        proposal = json.dumps(dict(proposal_seq=paxos_seq)).encode()
        res = await Client.multi_post('paxos/promise/{}'.format(seq), proposal)
        res = [v for v in res.values() if 'OK' == v['status']]
        if ARGS.quorum > len(res):
            return sanic.response.text('NO_PROMISE_QUORUM', status=503)

        proposal = (0, False)
        for v in res:
            if v['accepted_seq'] > proposal[0]:
                proposal = (v['accepted_seq'], v['accepted_val'])

        print(proposal)
        proposed_val = proposal[1]
        proposal = dict(proposal_seq=paxos_seq, proposal_val=proposal[1])
        proposal = json.dumps(proposal).encode()

        res = await Client.multi_post('paxos/accept/{}'.format(seq), proposal)
        if ARGS.quorum > len([v for v in res.values() if 'OK' == v['status']]):
            return sanic.response.text('NO_ACCEPT_QUORUM', status=503)

        res = await Client.multi_post('paxos/learn/{}'.format(seq), proposal)
        if ARGS.quorum > len([v for v in res.values() if 'OK' == v['status']]):
            return sanic.response.text('NO_LEARN_QUORUM', status=503)

        paxos['learned_val'] = proposed_val

    if paxos['learned_val'] is False:
        return sanic.response.raw(b'', headers={'x-msgbox-seq': seq})

    if paxos['learned_val'] is True and os.path.isfile(blobpath):
        with open(blobpath, 'rb') as fd:
            return sanic.response.raw(fd.read(), headers={'x-msgbox-seq': seq})

    return sanic.exceptions.SanicException('RETRY')


def allowed(request):
    auth_key = request.headers.get('x-auth-key', None)
    if not auth_key:
        return False

    if auth_key == ARGS.auth_key:
        return True

    return False


class Client:
    @classmethod
    async def get(self, url):
        async with aiohttp.ClientSession(headers=self.headers) as s:
            async with s.get(url, ssl=False) as r:
                return dict(status=r.status, headers=r.headers,
                            json=await r.json())

    @classmethod
    async def get_blob(self, url):
        async with aiohttp.ClientSession(headers=self.headers) as s:
            try:
                async with s.get(url, ssl=False) as r:
                    return dict(status=r.status, headers=r.headers,
                                blob=await r.read())
            except Exception:
                return dict(status=500)

    @classmethod
    async def multi_get(self, url):
        responses = await asyncio.gather(*[
            asyncio.ensure_future(
                self.get('{}/{}'.format(s, url))) for s in ARGS.servers],
            return_exceptions=True)

        return self.combine_result(ARGS.servers, responses)

    @classmethod
    async def post(self, url, data):
        async with aiohttp.ClientSession(headers=self.headers) as s:
            async with s.post(url, data=data, ssl=False) as r:
                return dict(status=r.status, headers=r.headers,
                            json=await r.json())

    @classmethod
    async def multi_post(self, url, data):
        responses = await asyncio.gather(*[
            asyncio.ensure_future(
                self.post('{}/{}'.format(s, url), data))
            for s in ARGS.servers],
            return_exceptions=True)

        return self.combine_result(ARGS.servers, responses)

    @classmethod
    def combine_result(self, urls, responses):
        result = dict()
        for s, r in zip(urls, responses):
            if type(r) is dict and 200 == r['status']:
                result[s] = r['json']
        return result


if '__main__' == __name__:
    ARGS = argparse.ArgumentParser()
    ARGS.add_argument('--port', dest='port', type=int)
    ARGS.add_argument('--index', dest='index', type=int)
    ARGS.add_argument('--conf', dest='conf', default='config.json')
    ARGS = ARGS.parse_args()

    with open(ARGS.conf) as fd:
        config = json.load(fd)

    ARGS.auth_key = config['auth_key']
    ARGS.servers = config['servers']
    ARGS.quorum = int(len(ARGS.servers)/2) + 1
    Client.headers = {'x-auth-key': ARGS.auth_key}

    # Find the maximum seq written so far
    path = 'data'
    os.makedirs(path, exist_ok=True)
    for i in range(3):
        filenames = [int(c) for c in os.listdir(path) if c.isdigit()]
        if filenames:
            path = os.path.join(path, str(max(filenames)))
        else:
            path = os.path.join(path, '0')
            os.makedirs(path)

    filenames = [int(c) for c in os.listdir(path) if c.isdigit()]
    path = os.path.join(path, str(max(filenames) if filenames else 0))
    G.max_seq = 0
    for p in path.split('/')[1:]:
        G.max_seq = G.max_seq*10**2 + int(p)

    APP.run(port=ARGS.port, single_process=True, access_log=True)
