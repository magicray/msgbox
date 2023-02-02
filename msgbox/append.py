import sys
import json
import msgbox


def main(conf_file, path, blob):
    client = msgbox.Client(conf_file)

    result = client.append(path, blob)
    json_dump = json.dumps(result, indent=4, sort_keys=True)
    sys.stderr.write(json_dump + '\n')


if '__main__' == __name__:
    main(sys.argv[1], sys.argv[2], sys.stdin.read())
