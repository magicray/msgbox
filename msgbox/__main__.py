import sys
import time
import json
import msgbox
import hashlib


def append(client, blob):
    result = client.append(blob)

    sys.stderr.write(json.dumps(result, indent=4, sort_keys=True) + '\n')


def tail(client, seq):
    chksum = ''
    for r in client.tail(seq):
        if 'blob' in r:
            blob = r.pop('blob', b'')
            if blob:
                x = hashlib.md5(blob).hexdigest()
                y = chksum + x
                y = hashlib.md5(y.encode()).hexdigest()
                print('{} {} {} {}'.format(y, x, r['seq'], len(blob)))
        else:
            time.sleep(1)


def put(client, key, value):
    result = client.put(key, value)

    sys.stderr.write(json.dumps(result, indent=4, sort_keys=True) + '\n')


if '__main__' == __name__:
    client = msgbox.Client(sys.argv[1].split(','))

    if 2 == len(sys.argv):
        append(client, sys.stdin.read())

    if 3 == len(sys.argv) and sys.argv[2].isdigit():
        tail(client, int(sys.argv[2]))

    if 3 == len(sys.argv) and not sys.argv[2].isdigit():
        put(client, sys.argv[2], sys.stdin.read())
