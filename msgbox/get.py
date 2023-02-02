import sys
import json
import msgbox


def main(conf_file, path):
    client = msgbox.Client(conf_file)

    result = client.get(path)
    if type(result) is dict:
        blob = result.pop('blob')
        json_dump = json.dumps(result, indent=4, sort_keys=True)
        sys.stderr.write(json_dump + '\n')
        sys.stdout.buffer.write(blob)
    else:
        sys.stderr.write(result + '\n')


if '__main__' == __name__:
    main(sys.argv[1], sys.argv[2])
