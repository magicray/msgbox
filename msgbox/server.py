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


APP = sanic.Sanic('logdb')
signal.alarm(random.randint(1, 5))


class G:
    max_seq = None
    start_time = time.strftime('%y%m%d-%H%M%S')


# Form a hierarchical path to avoid too many files in a directory
def seq2path(seq):
    batch_size = ARGS.batch

    one = int(seq / batch_size) % batch_size
    two = int(seq / batch_size**2) % batch_size
    three = int(seq / batch_size**3) % batch_size

    return os.path.join('data', str(three), str(two), str(one), str(seq))


# Atomic file creation. Write a tmp file and then move to final path
def blob_dump(path, blob):
    os.makedirs(os.path.dirname(path), exist_ok=True)

    tmppath = '{}-{}.tmp'.format(path, uuid.uuid4())

    with open(tmppath, 'wb') as fd:
        fd.write(blob)

    os.rename(tmppath, path)


# Write the blob with a unique filename
# Simulate a paxos accept request
@APP.post('/<seq:int>/<uuid:str>')
async def dump(request, seq, uuid):
    seq = int(seq)

    filepath = seq2path(seq)
    if not allowed(request) or os.path.isfile(filepath):
        raise sanic.exception.Unauthorized()

    G.max_seq = max(seq, G.max_seq)
    blobpath = os.path.join(os.path.dirname(filepath), uuid)

    blob_dump(blobpath, request.body)
    blob_dump(filepath, json.dumps(dict(
        promised_seq=1, accepted_seq=1,
        accepted_val=uuid), indent=4).encode())

    return sanic.response.json(dict(status='OK', max_seq=G.max_seq))


# If a majority replies, our accept request for this seq is successful
# On the read side, all paxos rounds, would be run to finalize the write
@APP.post('/')
async def append(request):
    n = len(ARGS.servers)
    G.max_seq = int((G.max_seq+n)/n)*n + ARGS.index
    seq = G.max_seq

    guid = str(uuid.uuid4())
    res = await Client.multi_post('{}/{}'.format(seq, guid), request.body)

    if ARGS.quorum > len([r for r in res.values() if r['status'] == 'OK']):
        raise sanic.exceptions.ServiceUnavailable()

    return sanic.response.json(seq, headers={
        'x-msgbox-seq': seq,
        'x-msgbox-length': len(request.body)})


@APP.post('/paxos/<phase:str>/<seq:int>')
async def paxos_server(request, phase, seq):
    seq = int(seq)

    if not allowed(request):
        raise sanic.exception.Unauthorized()

    G.max_seq = max(seq, G.max_seq)

    paxos = dict(promised_seq=0, accepted_seq=0, accepted_val=None)

    filepath = seq2path(seq)
    if os.path.isfile(filepath):
        with open(filepath) as fd:
            paxos = json.loads(fd.read())

    if 'learned_val' in paxos:
        return sanic.response.json(dict(
            status='OK',
            accepted_seq=10**15-1,
            accepted_val=paxos['learned_val']))

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

    if request.get('proposal_val', None):
        dirpath = os.path.dirname(seq2path(seq))
        blobpath = os.path.join(dirpath, request['proposal_val'])
        for srv in ARGS.servers:
            if not os.path.isfile(blobpath):
                url = '{}/{}/{}'.format(srv, seq, request['proposal_val'])
                res = await Client.get_blob(url)
                if 200 == res['status']:
                    blob_dump(blobpath, res['blob'])

        if not os.path.isfile(blobpath):
            return sanic.response.json(dict(status='BLOB_NOT_FOUND'))

    if 'accept' == phase and request['proposal_seq'] >= paxos['promised_seq']:
        blob_dump(filepath, json.dumps(dict(
            promised_seq=request['proposal_seq'],
            accepted_seq=request['proposal_seq'],
            accepted_val=request['proposal_val']), indent=4).encode())

        return sanic.response.json(dict(status='OK'))

    if 'learn' == phase and request['proposal_seq'] >= paxos['promised_seq']:
        blob_dump(filepath, json.dumps(dict(
            learned_val=request['proposal_val']), indent=4).encode())

        return sanic.response.json(dict(status='OK'))

    return sanic.response.json(dict(status='INVALID'))


@APP.get('/<seq:int>/<uuid:str>')
async def blob_read(request, seq, uuid):
    dirpath = os.path.dirname(seq2path(seq))
    blobpath = os.path.join(dirpath, uuid)

    if os.path.isfile(blobpath):
        with open(blobpath, 'rb') as fd:
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

    if 'learned_val' not in paxos:
        paxos_seq = int(time.time())

        proposal = json.dumps(dict(proposal_seq=paxos_seq)).encode()
        res = await Client.multi_post('paxos/promise/{}'.format(seq), proposal)
        res = [v for v in res.values() if 'OK' == v['status']]
        if ARGS.quorum > len(res):
            return sanic.response.text('NO_PROMISE_QUORUM', status=503)

        proposal = (0, None)
        for v in res:
            if v['accepted_seq'] > proposal[0]:
                proposal = (v['accepted_seq'], v['accepted_val'])

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

    if 'learned_val' in paxos:
        blob = b''
        if paxos['learned_val']:
            dirpath = os.path.dirname(seq2path(seq))
            blobpath = os.path.join(dirpath, paxos['learned_val'])

            with open(blobpath, 'rb') as fd:
                blob = fd.read()

        return sanic.response.raw(blob, headers={
            'x-msgbox-seq': seq,
            'x-msgbox-length': len(blob)})


def allowed(request):
    auth_key = request.headers.get('x-auth-key', None)
    if not auth_key:
        return False

    if auth_key == ARGS.auth_key:
        return True

    return False


class Client:
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
    async def post(self, url, data):
        async with aiohttp.ClientSession(headers=self.headers) as s:
            try:
                async with s.post(url, data=data, ssl=False) as r:
                    return dict(status=r.status, headers=r.headers,
                                json=await r.json())
            except Exception:
                return dict(status=500)

    @classmethod
    async def multi_post(self, url, data):
        responses = await asyncio.gather(*[
            asyncio.ensure_future(
                self.post('{}/{}'.format(s, url), data))
            for s in ARGS.servers],
            return_exceptions=True)

        result = dict()
        for s, r in zip(ARGS.servers, responses):
            if type(r) is dict and 200 == r['status']:
                result[s] = r['json']
        return result


if '__main__' == __name__:
    ARGS = argparse.ArgumentParser()
    ARGS.add_argument('--port', dest='port', type=int)
    ARGS.add_argument('--index', dest='index', type=int)
    ARGS.add_argument('--batch', dest='batch', type=int, default=100)
    ARGS.add_argument('--quorum', dest='quorum', type=int, default=0)
    ARGS.add_argument('--conf', dest='conf', default='config.json')
    ARGS = ARGS.parse_args()

    with open(ARGS.conf) as fd:
        config = json.load(fd)

    ARGS.auth_key = config['auth_key']
    ARGS.servers = config['servers']
    ARGS.quorum = max(ARGS.quorum, int(len(ARGS.servers)/2) + 1)
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
    G.max_seq = max(filenames) if filenames else 0

    APP.run(port=ARGS.port, single_process=True, access_log=True)
