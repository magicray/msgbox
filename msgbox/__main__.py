import sys
import time
import json
import msgbox
import hashlib


def append(client, blob):
    result = json.dumps(client.append(blob), indent=4, sort_keys=True)
    sys.stderr.write(result + '\n')


def tail(client, seq, step):
    chksum = ''
    for r in client.tail(seq, step):
        if 'blob' in r:
            blob = r.pop('blob', b'')
            if blob:
                x = hashlib.md5(blob).hexdigest()
                y = chksum + x
                y = hashlib.md5(y.encode()).hexdigest()
                res = 'log({}) blob({}) seq({}) len({})'.format(
                    y, x, r['seq'], len(blob))
                sys.stderr.write(res + '\n')
        else:
            time.sleep(1)


if '__main__' == __name__:
    client = msgbox.Client(sys.argv[1].split(','))

    if 2 == len(sys.argv):
        append(client, sys.stdin.read())

    if 3 == len(sys.argv):
        tail(client, int(sys.argv[2]), 1)

    if 4 == len(sys.argv):
        tail(client, int(sys.argv[2]), int(sys.argv[3]))
