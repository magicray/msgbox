import sys
import hashlib
import msgbox


def main(conf_file, seq):
    client = msgbox.Client(conf_file)

    for r in client.tail(seq):
        blob = r.pop('blob', b'')
        sys.stdout.write('{} {} {}\n'.format(
            hashlib.md5(blob).hexdigest(), r['seq'], len(blob)))


if '__main__' == __name__:
    main(sys.argv[1], int(sys.argv[2]))
