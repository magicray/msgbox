import sys
import json
import time
import hashlib
import msgbox


def main(conf_file, path, term, seq):
    client = msgbox.Client(conf_file)

    for r in client.tail(path, term, seq):
        if not r.get('blob', None):
            json_dump = json.dumps(r, indent=4, sort_keys=True)
            sys.stderr.write(json_dump + '\n')
            time.sleep(1)
        else:
            sys.stdout.write('{} {} {}/{}/{}\n'.format(
                hashlib.md5(r['blob']).hexdigest(), len(r['blob']),
                r['path'], r['term'], r['seq']))


if '__main__' == __name__:
    main(sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4])
