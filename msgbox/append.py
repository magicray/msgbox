import sys
import json
import msgbox


def main(conf_file, blob):
    client = msgbox.Client(conf_file)
    result = client.append(blob)

    sys.stderr.write(json.dumps(result, indent=4, sort_keys=True) + '\n')


if '__main__' == __name__:
    main(sys.argv[1], sys.stdin.read())
