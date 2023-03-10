import os
import sys
import time
import uuid
import sanic
import signal
import pickle
import random
import hashlib
import asyncio
import aiohttp
import logging


APP = sanic.Sanic('logdb')
signal.alarm(random.randint(1, 5))


# Global variables
class G:
    max_seq = None
    session = None

    # DateTime  - 'YYYYMMDD-HHMMSS'
    default_seq = '00000000-000000'


def allowed(request):
    if G.cluster_key == request.headers.get('x-auth-key', None):
        return True


def paxos_encode(promised_seq, accepted_seq):
    result = '{}\n{}\n'.format(promised_seq, accepted_seq).encode()
    assert(32 == len(result))
    return result


def paxos_decode(input_bytes):
    assert(32 == len(input_bytes))
    promised_seq, accepted_seq, _ = input_bytes.decode().split('\n')
    return promised_seq, accepted_seq


def response(obj):
    return sanic.response.raw(pickle.dumps(obj))


@APP.post('/seq/next')
async def next_seq(request):
    if allowed(request) is not True:
        raise sanic.exceptions.Unauthorized('Unauthorized Request')

    G.max_seq += 1
    return response(G.max_seq)


@APP.post('/<phase:str>/<proposal_seq:str>/<key:path>')
async def paxos_server(request, phase, proposal_seq, key):
    # DateTime  - 'YYYYMMDD-HHMMSS'
    learned_seq = '99999999-999999'

    if request is not None and allowed(request) is not True:
        raise sanic.exceptions.Unauthorized('Unauthorized Request')

    os.makedirs(os.path.dirname(key), exist_ok=True)
    tmpfile = '{}-{}.tmp'.format(key, uuid.uuid4())

    promised_seq = accepted_seq = G.default_seq
    if os.path.isfile(key):
        with open(key, 'rb') as fd:
            promised_seq, accepted_seq = paxos_decode(fd.read(32))

            if request is None:
                return fd.read() if learned_seq == promised_seq else None

            if learned_seq == promised_seq == accepted_seq:
                # Value for this key has already been learned
                # Just play along and respond to any new paxos rounds
                # to help the nodes that do not have this value yet.
                #
                # Respond to promise/accept/learn requests normally,
                # without updating anything. Return the largest possible
                # accepted_seq number, so that this value is proposed by
                # the node that initiated this round.
                if 'promise' == phase:
                    return response([accepted_seq, fd.read()])

                return response('OK')

    if 'promise' == phase and proposal_seq > promised_seq:
        # Update the header if file already exists.
        if os.path.isfile(key):
            with open(key, 'r+b') as fd:
                fd.write(paxos_encode(proposal_seq, accepted_seq))

        # Atomically create a new file if it doesn't
        else:
            with open(tmpfile, 'wb') as fd:
                fd.write(paxos_encode(proposal_seq, accepted_seq))
            os.rename(tmpfile, key)

        with open(key, 'rb') as fd:
            promised_seq, accepted_seq = paxos_decode(fd.read(32))
            return response([accepted_seq, fd.read()])

    if 'accept' == phase and proposal_seq == promised_seq:
        # Atomically write the header and accepted value by creating
        # a tmp file and then renaming it.
        with open(tmpfile, 'wb') as fd:
            fd.write(paxos_encode(proposal_seq, proposal_seq))
            fd.write(pickle.loads(request.body))
        os.rename(tmpfile, key)

        return response('OK')

    if 'learn' == phase and proposal_seq == promised_seq == accepted_seq:
        # Mark this value as final.
        # promise_seq = accepted_seq = '99999999-999999'
        # This is the largest possible value for seq and would ensure
        # tha any subsequent paxos rounds for this key accept only this value.
        with open(key, 'r+b') as fd:
            fd.write(paxos_encode(learned_seq, learned_seq))

        return response('OK')


async def rpc(url, obj=None):
    if G.session is None:
        G.session = aiohttp.ClientSession(
            connector=aiohttp.TCPConnector(limit=1000),
            headers={'x-auth-key': G.cluster_key})

    responses = await asyncio.gather(
        *[asyncio.ensure_future(
          G.session.post('https://{}/{}'.format(s, url),
                         data=pickle.dumps(obj), ssl=False))
          for s in G.servers],
        return_exceptions=True)

    result = dict()
    for s, r in zip(G.servers, responses):
        if type(r) is aiohttp.client_reqrep.ClientResponse:
            if 200 == r.status:
                result[s] = pickle.loads(await r.read())

    return result


async def paxos_client(key, value):
    seq_key = '{}/{}'.format(time.strftime('%Y%m%d-%H%M%S'), key)

    res = await rpc('promise/{}'.format(seq_key))
    if G.quorum > len(res):
        return 'NO_PROMISE_QUORUM'

    proposal = (G.default_seq, value)
    for srv, (accepted_seq, accepted_val) in res.items():
        if accepted_seq > proposal[0]:
            proposal = (accepted_seq, accepted_val)

    if G.quorum > len(await rpc('accept/{}'.format(seq_key), proposal[1])):
        return 'NO_ACCEPT_QUORUM'

    if G.quorum > len(await rpc('learn/{}'.format(seq_key))):
        return 'NO_LEARN_QUORUM'

    return 'CONFLICT' if value is not proposal[1] else 'OK'


# Form a hierarchical path to avoid too many files in a directory
def seq2path(seq):
    batch_size = 100

    one = str(int(seq / batch_size) % batch_size)
    two = str(int(seq / batch_size**2) % batch_size)
    three = str(int(seq / batch_size**3) % batch_size)

    return os.path.join('data', 'log', three, two, one, str(seq))


@APP.post('/')
async def append(request):
    res = await rpc('seq/next')
    seq = max([obj for obj in res.values()])

    if 'OK' == await paxos_client(seq2path(seq), request.body):
        return sanic.response.json(seq, headers={
            'x-server-seq': seq,
            'x-server-length': len(request.body)})


@APP.get('/<seq:int>')
async def tail(request, seq):
    seq = int(seq)
    key = seq2path(seq)

    if seq > G.max_seq:
        await asyncio.sleep(1)
        raise sanic.exceptions.NotFound()

    for i in range(2):
        blob = await paxos_server(None, None, seq, key)

        if blob is not None:
            return sanic.response.raw(blob, headers={
                'x-server-seq': seq,
                'x-server-length': len(blob)})

        await paxos_client(key, b'')


if '__main__' == __name__:
    G.servers = set()
    for i in range(1, len(sys.argv)):
        G.servers.add(sys.argv[i])

    G.host, G.port = sys.argv[1].split(':')
    G.port = int(G.port)

    with open('cluster.key') as fd:
        G.cluster_key = fd.read().strip() + ''.join(sorted(G.servers))
        G.cluster_key = hashlib.md5(G.cluster_key.encode()).hexdigest()

    G.quorum = int(len(G.servers)/2) + 1

    os.makedirs(os.path.join('data', 'log'), exist_ok=True)

    # Find the maximum log seq written so far
    path = os.path.join('data', 'log')
    for i in range(3):
        filenames = [int(c) for c in os.listdir(path) if c.isdigit()]
        path = os.path.join(path, str(max(filenames)) if filenames else '0')
        os.makedirs(path, exist_ok=True)

    filenames = [int(c) for c in os.listdir(path) if c.isdigit()]
    G.max_seq = max(filenames) if filenames else 0

    logging.critical('Starting server : {}:{}'.format(G.host, G.port))
    for i, srv in enumerate(sorted(G.servers)):
        logging.critical('cluster node({}) : {}'.format(i+1, srv))

    APP.run(host=G.host, port=G.port, single_process=True, access_log=True,
            ssl=dict(cert='ssl.crt', key='ssl.key', names=['*']))
