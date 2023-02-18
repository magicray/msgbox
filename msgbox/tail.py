import sys
import hashlib
import msgbox


def main(servers, seq):
    client = msgbox.Client(servers)

    for r in client.tail(seq):
        blob = r.pop('blob', b'')
        sys.stdout.write('{} {} {}\n'.format(
            hashlib.md5(blob).hexdigest(), r['seq'], len(blob)))


if '__main__' == __name__:
    main(sys.argv[1].split(','), int(sys.argv[2]))
