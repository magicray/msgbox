import os
import time
import json
import uuid
import sanic
import signal
import struct
import pickle
import random
import asyncio
import aiohttp
import argparse


APP = sanic.Sanic('logdb')
signal.alarm(random.randint(1, 5))


# Global variables
class G:
    max_seq = None
    session = None


def allowed(request):
    if ARGS.auth_key == request.headers.get('x-auth-key', None):
        return True


@APP.post('/next/seq')
async def next_seq(request):
    if allowed(request) is not True:
        raise sanic.exceptions.Unauthorized('Unauthorized Request')

    G.max_seq += 1
    return sanic.response.raw(pickle.dumps(G.max_seq))


@APP.post('/<phase:str>/<proposal_seq:int>/<key:path>')
async def paxos_server(request, phase, proposal_seq, key):
    proposal_seq = int(proposal_seq)

    if allowed(request) is not True:
        raise sanic.exceptions.Unauthorized('Unauthorized Request')

    os.makedirs(os.path.dirname(key), exist_ok=True)

    promised_seq, accepted_seq = 0, 0
    if os.path.isfile(key):
        with open(key, 'rb') as fd:
            promised_seq, accepted_seq = struct.unpack('!II', fd.read(8))

            if 0 == promised_seq and 0 == accepted_seq:
                return sanic.response.raw(pickle.dumps([10**15, fd.read()]))

    if 'promise' == phase and proposal_seq > promised_seq:
        mode = 'r+b' if os.path.isfile(key) else 'w+b'

        with open(key, mode) as fd:
            fd.write(struct.pack('!II', proposal_seq, accepted_seq))

            return sanic.response.raw(pickle.dumps([accepted_seq, fd.read()]))

    if 'accept' == phase and proposal_seq == promised_seq:
        tmpfile = '{}-{}.tmp'.format(key, uuid.uuid4())
        with open(tmpfile, 'wb') as fd:
            fd.write(struct.pack('!II', proposal_seq, proposal_seq))
            fd.write(request.body)
        os.rename(tmpfile, key)

        return sanic.response.raw(b'OK')

    if 'learn' == phase and proposal_seq == promised_seq:
        with open(key, 'r+b') as fd:
            fd.write(struct.pack('!II', 0, 0))

        return sanic.response.raw(b'OK')


async def rpc(url, blob=None):
    if G.session is None:
        G.session = aiohttp.ClientSession(headers=G.auth_header)

    responses = await asyncio.gather(
        *[asyncio.ensure_future(
          G.session.post('{}/{}'.format(s, url), data=blob, ssl=False))
          for s in ARGS.servers],
        return_exceptions=True)

    result = dict()
    for s, r in zip(ARGS.servers, responses):
        if type(r) is aiohttp.client_reqrep.ClientResponse:
            if 200 == r.status:
                result[s] = await r.read()

    return result


async def paxos_client(key, value):
    paxos_seq = int(time.time())

    res = await rpc('promise/{}/{}'.format(paxos_seq, key))
    if ARGS.quorum > len(res):
        return 'NO_PROMISE_QUORUM'

    proposal = (0, value)
    for srv, body in res.items():
        accepted_seq, accepted_val = pickle.loads(body)
        if accepted_seq > proposal[0]:
            proposal = (accepted_seq, accepted_val)

    res = await rpc('accept/{}/{}'.format(paxos_seq, key), proposal[1])
    if ARGS.quorum > len(res):
        return 'NO_ACCEPT_QUORUM'

    res = await rpc('learn/{}/{}'.format(paxos_seq, key))
    if ARGS.quorum > len(res):
        return 'NO_LEARN_QUORUM'

    return 'CONFLICT' if value is not proposal[1] else 'OK'


# Form a hierarchical path to avoid too many files in a directory
def seq2path(seq):
    batch_size = ARGS.batch

    one = str(int(seq / batch_size) % batch_size)
    two = str(int(seq / batch_size**2) % batch_size)
    three = str(int(seq / batch_size**3) % batch_size)

    return os.path.join('data', 'log', three, two, one, str(seq))


@APP.post('/')
async def append(request):
    res = await rpc('next/seq')
    if ARGS.quorum > len(res):
        return 'NO_QUORUM'

    seq = max([pickle.loads(body) for body in res.values()])

    if 'OK' == await paxos_client(seq2path(seq), request.body):
        return sanic.response.json(seq, headers={
            'x-logdb-seq': seq,
            'x-logdb-length': len(request.body)})


@APP.get('/<seq:int>')
async def tail(request, seq):
    seq = int(seq)

    if seq > G.max_seq:
        await asyncio.sleep(1)
        raise sanic.exceptions.NotFound()

    key = seq2path(seq)

    for i in range(2):
        if os.path.isfile(key):
            with open(key, 'rb') as fd:
                promised_seq, accepted_seq = struct.unpack('!II', fd.read(8))

                if 0 == promised_seq and 0 == accepted_seq:
                    blob = fd.read()
                    return sanic.response.raw(blob, headers={
                        'x-logdb-seq': seq,
                        'x-logdb-length': len(blob)})

        await paxos_client(key, b'')


@APP.put('/<key:path>/<version:int>')
async def put(request, key, version):
    version = int(version)

    key = os.path.join('data', 'kv', key, str(version))
    status = await paxos_client(key, request.body)

    return sanic.response.json(status, headers={
        'x-logdb-length': len(request.body)})


@APP.get('/<key:path>')
async def get(request, key):
    key = os.path.join('data', 'kv', key)

    return sanic.response.json(key, headers={
        'x-logdb-length': len(request.body)})


if '__main__' == __name__:
    ARGS = argparse.ArgumentParser()
    ARGS.add_argument('--port', dest='port', type=int)
    ARGS.add_argument('--batch', dest='batch', type=int, default=100)
    ARGS.add_argument('--quorum', dest='quorum', type=int, default=0)
    ARGS.add_argument('--conf', dest='conf', default='config.json')
    ARGS = ARGS.parse_args()

    with open(ARGS.conf) as fd:
        config = json.load(fd)

    ARGS.auth_key = config['auth_key']
    ARGS.servers = config['servers']
    ARGS.quorum = max(ARGS.quorum, int(len(ARGS.servers)/2) + 1)
    G.auth_header = {'x-auth-key': ARGS.auth_key}

    os.makedirs(os.path.join('data', 'kv'), exist_ok=True)
    os.makedirs(os.path.join('data', 'log'), exist_ok=True)

    # Find the maximum log seq written so far
    path = os.path.join('data', 'log')
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
