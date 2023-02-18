import sys
import json
import msgbox


def main(servers, blob):
    client = msgbox.Client(servers)
    result = client.append(blob)

    sys.stderr.write(json.dumps(result, indent=4, sort_keys=True) + '\n')


if '__main__' == __name__:
    main(sys.argv[1].split(','), sys.stdin.read())
