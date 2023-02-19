import sys
import time
import msgbox
import hashlib


def main(servers, seq):
    client = msgbox.Client(servers)

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
    main(sys.argv[1].split(','), int(sys.argv[2]))
