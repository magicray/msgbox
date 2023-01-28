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
start_time = time.strftime('%y%m%d-%H%M%S')
signal.alarm(random.randint(1, 10))

TERMS = dict()
READER_INFO = dict()
WRITE_LOCKS = dict()
WRITE_LEADERS = dict()


# All files and directory should be in the assigned location
def abspath(*args):
    args = [str(a) for a in args]
    return os.path.join(os.getcwd(), 'data', *args)


# Largest filename in a dir
def largest_filename(path):
    path = abspath(path)

    filenames = list()
    if os.path.isdir(path):
        filenames = [int(c) for c in os.listdir(path) if c.isdigit()]

    return max(filenames) if filenames else -1


# Read the content of a file
def blob_load(path, term, seq):
    with open(abspath(path, term, seq), 'rb') as fd:
        return fd.read()


# Atomic file creation. Write a tmp file and then move to final path
def blob_dump(path, term, seq, blob):
    term_dir = abspath(path, term)
    if not os.path.isdir(term_dir):
        os.makedirs(term_dir)

    path = abspath(path, term, seq)
    tmppath = '{}-{}.tmp'.format(path, uuid.uuid4())

    with open(tmppath, 'wb') as fd:
        fd.write(blob)

    os.rename(tmppath, path)


# We vote only once for a term in our lifetime
# Agree if and only if this term is biggest seen so far
@APP.get('/vote/<path:path>/<term:int>')
async def vote(request, path, term):
    term = int(term)

    if not allowed(request):
        return sanic.response.json('Unauthorized', status=401)

    TERMS.pop(path, None)

    if os.path.isdir(abspath(path, term)):
        return sanic.response.json(dict(status='OLD_TERM'))

    os.makedirs(abspath(path, term))
    TERMS[path] = largest_filename(path)

    if term == TERMS[path]:
        return sanic.response.json(dict(status='OK'))

    return sanic.response.json(dict(status='OBSOLETED'))


@APP.get('/info/<path:path>')
async def path_info(request, path):
    TERMS[path] = largest_filename(path)

    return sanic.response.json(dict(
        path=path,
        term=TERMS[path],
        leader=WRITE_LEADERS.get(path, None)))


@APP.get('/leader/<path:path>')
async def leader(request, path):
    for i in range(2):
        # Lets find if we have an active leader
        res = await Client.multi_get('info/{}'.format(path))

        # Found a leader. Could be from an older term though.
        # Even if it is from an older term, we are safe as
        # any writes through this would fail.
        for k, v in res.items():
            if v['leader'] is not None:
                return sanic.response.json(dict(
                    path=path, leader=k,
                    term=v['leader']['term'],
                    seq=v['leader']['seq'],
                    responses=res))

        # Leader not found. Lets find the max term handled so far
        # Can't conclude anything without hearing from a majority
        if len(res) < ARGS.quorum:
            break

        # Lets solicit vote for this node as the leader for the next term
        term = max([r['term'] for r in res.values()])
        res = await Client.multi_get('vote/{}/{}'.format(path, term+1))

        votes = [True for r in res.values() if 'OK' == r['status']]

        # A Majority did not vote for us. This term is abandoned
        if len(votes) < ARGS.quorum:
            break

        # This node got votes from a majority. Lets mark it as the leader
        abspath = os.path.join(os.getcwd(), 'audit', 'leader')
        if not os.path.isdir(abspath):
            os.makedirs(abspath)

        with open(os.path.join(abspath, start_time), 'a') as fd:
            fd.write('{}/{}\n'.format(path, term+1))

        WRITE_LEADERS[path] = dict(term=term+1, seq=0)

    return sanic.response.json(
        dict(status='SERVICE_UNAVAILABLE', leader=None),
        status=503)


# Write a new blob. Invalidate the cached term if this one is larger
@APP.post('/blob/<path:path>/<term:int>/<seq:int>')
async def blob_write(request, path, term, seq):
    term, seq = int(term), int(seq)

    if not allowed(request):
        return sanic.response.json('Unauthorized', status=401)

    if path not in TERMS:
        TERMS[path] = largest_filename(path)

    if term < TERMS[path]:
        return sanic.response.json(dict(status='OLD_TERM'))

    blob_dump(path, term, seq, request.body)
    TERMS[path] = term

    return sanic.response.json(dict(status='OK'))


@APP.post('/msgbox/<path:path>')
async def msgbox_post(request, path):
    async with WRITE_LOCKS.setdefault(path, asyncio.Lock()):
        leader = WRITE_LEADERS.pop(path, None)

        if leader is None:
            return sanic.response.json(dict(status='NOT_A_LEADER'), status=400)

        url = 'blob/{}/{}/{}'.format(path, leader['term'], leader['seq'])
        res = await Client.multi_post(url, request.body)
        ok = [True for r in res.values() if r['status'] == 'OK']

        # Not acked by a quorum of writers
        if len(ok) < ARGS.quorum:
            return sanic.response.json(
                dict(status='SERVICE_UNAVAILABLE', responses=res),
                status=503)

        # Write is successful
        leader['seq'] += 1
        WRITE_LEADERS[path] = leader
        return sanic.response.json(dict(
            status='OK', path=path, term=leader['term'],
            seq=leader['seq']-1, length=len(request.body)))


@APP.get('/blob/<path:path>/<term:int>/<seq:int>')
async def blob_read(request, path, term, seq):
    if os.path.isfile(abspath(path, term, seq)):
        return sanic.response.raw(blob_load(path, term, seq))

    return sanic.response.raw(b'', status=404)


@APP.post('/paxos/<phase:str>/<path:path>/<term:int>')
async def paxos_server(request, phase, path, term):
    term = int(term)

    if os.path.isfile(abspath(path, term, 'paxos')):
        with open(abspath(path, term, 'paxos')) as fd:
            paxos = json.loads(fd.read())
    else:
        paxos = dict(promised_seq=0, accepted_seq=0, accepted_val=None)

    if 'learned_val' in paxos:
        return sanic.response.json(dict(
            status='LEARNED',
            learned_val=paxos['learned_val']))

    if term >= largest_filename(abspath(path)):
        return sanic.response.json(dict(
            status='STILL_FOLLOWING_THE_OLD_LEADER',
            max_seq=largest_filename(abspath(path, term))))

    request = request.json

    if 'promise' == phase and request['proposal_seq'] > paxos['promised_seq']:
        blob_dump(path, term, 'paxos', json.dumps(dict(
            promised_seq=request['proposal_seq'],
            accepted_seq=paxos['accepted_seq'],
            accepted_val=paxos['accepted_val']), indent=4).encode())

        return sanic.response.json(dict(
            status='OK',
            accepted_seq=paxos['accepted_seq'],
            accepted_val=paxos['accepted_val'],
            # This is not part of paxos. Just piggyback to avoid another call.
            max_seq=largest_filename(abspath(path, term))))

    if 'accept' == phase and request['proposal_seq'] >= paxos['promised_seq']:
        blob_dump(path, term, 'paxos', json.dumps(dict(
            promised_seq=request['proposal_seq'],
            accepted_seq=request['proposal_seq'],
            accepted_val=request['proposal_val']), indent=4).encode())

        return sanic.response.json(dict(status='OK'))

    if 'learn' == phase and request['proposal_seq'] >= paxos['promised_seq']:
        blob_dump(path, term, 'paxos', json.dumps(dict(
            promised_seq=request['proposal_seq'],
            accepted_seq=request['proposal_seq'],
            accepted_val=request['proposal_val'],
            learned_val=request['proposal_val']), indent=4).encode())

        return sanic.response.json(dict(status='OK'))

    return sanic.response.json(dict(status='OLD_SEQ'), status=400)


@APP.get('/msgbox/<path:path>/<term:int>/<seq:int>')
async def paxos_client(request, path, term, seq):
    seq, term = int(seq), int(term)

    term_state = READER_INFO.setdefault(
        (path, term),
        dict(closed=False, committed=-1))

    # Download from other nodes if we don't have it yet
    url = 'blob/{}/{}/{}'.format(path, term, seq)
    for srv in ARGS.servers:
        if not os.path.isfile(abspath(path, term, seq)):
            res = await Client.get_blob('{}/{}'.format(srv, url))

            if 200 == res['status']:
                blob_dump(path, term, seq, res['blob'])

    if not term_state['closed'] and seq > term_state['committed']:
        paxos_seq = int(time.time()*10**6)

        proposal = json.dumps(dict(proposal_seq=paxos_seq)).encode()

        url = 'paxos/promise/{}/{}'.format(path, term)
        res = await Client.multi_post(url, proposal)
        for v in res.values():
            if 'LEARNED' == v['status']:
                term_state['closed'] = True
                term_state['committed'] = v['learned_val']

    if not term_state['closed'] and seq > term_state['committed']:
        if ARGS.quorum > len(res):
            return sanic.response.json('NO_PROMISE_QUORUM', status=500)

        max_seq = max([v['max_seq'] for v in res.values()])
        max_seq_set = set([v['max_seq'] for v in res.values()])

        res = [v for v in res.values() if 'OK' == v['status']]
        if ARGS.quorum > len(res):
            if 1 == len(max_seq_set):
                term_state['committed'] = max_seq
            else:
                term_state['committed'] = max_seq - 1

    # There is already a new leader and this term is obsolete
    # At least a majority of nodes have the same max seq for this term
    # This term can be safely finalized with the above seq as the final file
    if not term_state['closed'] and seq > term_state['committed']:
        if len(res) >= ARGS.quorum and 1 == len(max_seq_set):
            proposal = (0, max_seq)
            for val in res:
                if val['accepted_seq'] > proposal[0]:
                    proposal = (val['accepted_seq'], val['accepted_val'])

            proposal = dict(proposal_seq=paxos_seq, proposal_val=proposal[1])
            proposal = json.dumps(proposal).encode()

            url = 'paxos/accept/{}/{}'.format(path, term)
            if ARGS.quorum > len(await Client.multi_post(url, proposal)):
                return sanic.response.json('NO_ACCEPT_QUORUM', status=500)

            url = 'paxos/learn/{}/{}'.format(path, term)
            if ARGS.quorum > len(await Client.multi_post(url, proposal)):
                return sanic.response.json('NO_LEARN_QUORUM', status=500)

            # Paxos round completed. This is the new committed value
            term_state['closed'] = True
            term_state['committed'] = max_seq

    # This term is closed.
    # Requested seq number would never be there as the leader is gone.
    # Redirect to first file in the next term
    if term_state['closed'] and seq > term_state['committed']:
        return sanic.response.redirect('/msgbox/{}/{}/0'.format(path, term+1))

    # File with the request seq is not yet written. User should retry
    if seq > term_state['committed']:
        return sanic.response.json('OUT_OF_RANGE', status=400)

    # We are good to return the file for this seq
    return sanic.response.raw(blob_load(path, term, seq))


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
            async with s.get(url, ssl=False) as r:
                return dict(status=r.status, headers=r.headers,
                            blob=await r.read())

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
    ARGS.add_argument('--conf', dest='conf', default='config.json')
    ARGS = ARGS.parse_args()

    with open(ARGS.conf) as fd:
        config = json.load(fd)

    ARGS.auth_key = config['auth_key']
    ARGS.servers = config['servers']
    ARGS.quorum = int(len(ARGS.servers)/2) + 1
    Client.headers = {'x-auth-key': ARGS.auth_key}

    APP.run(port=ARGS.port, single_process=True, access_log=True)
