import sys
import time
import json
import msgbox
import hashlib


def append(client, blob):
    result = client.append(blob)
    print(result)

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


if '__main__' == __name__:
    client = msgbox.Client(sys.argv[1].split(','))

    if 2 == len(sys.argv):
        append(client, sys.stdin.read())

    if 3 == len(sys.argv):
        tail(client, int(sys.argv[2]))
