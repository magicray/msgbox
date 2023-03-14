import os
import sys
import ssl
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


# Global variables
class G:
    seq = None
    ssl_ctx = None
    session = None
    lock = asyncio.Lock()


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


@APP.post('/seq-max')
async def seq_max(request):
    return response(G.seq)


@APP.post('/seq-next')
async def seq_next(request):
    G.seq += 1
    return response(G.seq)


@APP.post('/<phase:str>/<proposal_seq:str>/<log_seq:int>')
async def paxos_server(request, phase, proposal_seq, log_seq):
    # Format    - 'YYYYMMDD-HHMMSS'
    default_seq = '00000000-000000'
    learned_seq = '99999999-999999'

    # File path for this log entry
    log_seq = int(log_seq)
    path = os.path.join('data', str(int(log_seq / 10000)), str(log_seq))

    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmpfile = '{}-{}.tmp'.format(path, uuid.uuid4())

    promised_seq = accepted_seq = default_seq
    if os.path.isfile(path):
        with open(path, 'rb') as fd:
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
        if os.path.isfile(path):
            with open(path, 'r+b') as fd:
                fd.write(paxos_encode(proposal_seq, accepted_seq))

        # Atomically create a new file if it doesn't
        else:
            with open(tmpfile, 'wb') as fd:
                fd.write(paxos_encode(proposal_seq, accepted_seq))
            os.rename(tmpfile, path)

        with open(path, 'rb') as fd:
            promised_seq, accepted_seq = paxos_decode(fd.read(32))
            return response([accepted_seq, fd.read()])

    if 'accept' == phase and proposal_seq == promised_seq:
        # Atomically write the header and accepted value by creating
        # a tmp file and then renaming it.
        with open(tmpfile, 'wb') as fd:
            fd.write(paxos_encode(proposal_seq, proposal_seq))
            fd.write(pickle.loads(request.body))
        os.rename(tmpfile, path)

        return response('OK')

    if 'learn' == phase and proposal_seq == promised_seq == accepted_seq:
        # Mark this value as final.
        # promise_seq = accepted_seq = '99999999-999999'
        # This is the largest possible value for seq and would ensure
        # tha any subsequent paxos rounds for this key accept only this value.
        with open(path, 'r+b') as fd:
            fd.write(paxos_encode(learned_seq, learned_seq))

        return response('OK')


async def rpc(url, obj=None):
    if G.session is None:
        G.ssl_ctx = ssl.create_default_context(
            cafile='ca.pem',
            purpose=ssl.Purpose.SERVER_AUTH)
        G.ssl_ctx.load_cert_chain('server.pem', 'server.key')
        G.ssl_ctx.verify_mode = ssl.CERT_REQUIRED

        G.session = aiohttp.ClientSession(
            connector=aiohttp.TCPConnector(limit=1000))

    responses = await asyncio.gather(
        *[asyncio.ensure_future(
          G.session.post('https://{}/{}'.format(s, url),
                         data=pickle.dumps(obj), ssl=G.ssl_ctx))
          for s in G.servers],
        return_exceptions=True)

    result = dict()
    for s, r in zip(G.servers, responses):
        if type(r) is aiohttp.client_reqrep.ClientResponse:
            if 200 == r.status:
                result[s] = pickle.loads(await r.read())

    return result


# Standard PAXOS Propose flow
async def paxos_client(key, value):
    seq_key = '{}/{}'.format(time.strftime('%Y%m%d-%H%M%S'), key)

    res = await rpc('promise/{}'.format(seq_key))
    if G.quorum > len(res):
        return 'NO_PROMISE_QUORUM'

    proposal = ('00000000-000000', value)
    for srv, (accepted_seq, accepted_val) in res.items():
        if accepted_seq > proposal[0]:
            proposal = (accepted_seq, accepted_val)

    if G.quorum > len(await rpc('accept/{}'.format(seq_key), proposal[1])):
        return 'NO_ACCEPT_QUORUM'

    if G.quorum > len(await rpc('learn/{}'.format(seq_key))):
        return 'NO_LEARN_QUORUM'

    return 'CONFLICT' if value is not proposal[1] else 'OK'


@APP.post('/')
async def append(request):
    res = await rpc('seq-next')
    seq = max([num for num in res.values()])

    if 'OK' == await paxos_client(seq, request.body):
        return sanic.response.json(seq, headers={'x-seq': seq})


@APP.get('/<seq:int>')
async def tail(request, seq):
    seq = int(seq)

    # Let's wait for 30 seconds if this log entry does not yet exist
    for i in range(30):
        if seq <= G.seq:
            break

        # Take a lock to stop all the clients from trying,
        # Just one check is sufficient, as all were waiting for
        # the same log entry.
        async with G.lock:
            if seq > G.seq:
                await asyncio.sleep(1)
                res = await rpc('seq-max')
                G.seq = max([G.seq] + [num for num in res.values()])

    # This log entry was not found in 30 seconds
    if seq > G.seq:
        return

    # This log entry exist in the cluster
    for i in range(2):
        # Read the currently learned value
        blob = await paxos_server(None, None, None, seq)

        # This node has the learned value
        if blob is not None:
            return sanic.response.raw(blob, headers={'x-seq': seq})

        # This node hasn't yet learned the value.
        # Lets run a paxos round. This node will either learn
        # the value, if cluster has already leaned it.
        #
        # Otherwise, value would be set to empty byte array.
        await paxos_client(seq, b'')


if '__main__' == __name__:
    G.servers = set()
    for i in range(1, len(sys.argv)):
        G.servers.add(sys.argv[i])

    G.host, G.port = sys.argv[1].split(':')
    G.port = int(G.port)
    G.quorum = int(len(G.servers)/2) + 1

    G.seq = 0
    os.makedirs('data', exist_ok=True)

    # Find out the latest file
    for d in sorted([int(x) for x in os.listdir('data')], reverse=True):
        path = os.path.join('data', str(d))
        files = [int(x) for x in os.listdir(path) if x.isdigit()]
        if files:
            G.seq = max(files)
            break

    for i, srv in enumerate(sorted(G.servers)):
        logging.critical('cluster node({}) : {}'.format(i+1, srv))
    logging.critical('server({}:{}) seq({})'.format(G.host, G.port, G.seq))

    ssl_ctx = ssl.create_default_context(
        cafile='ca.pem',
        purpose=ssl.Purpose.CLIENT_AUTH)
    ssl_ctx.load_cert_chain('server.pem', 'server.key')
    ssl_ctx.verify_mode = ssl.CERT_REQUIRED

    signal.alarm(random.randint(1, 9))
    APP.run(host=G.host, port=G.port, single_process=True, access_log=True,
            ssl=ssl_ctx)
