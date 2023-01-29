import sys
import time
import hashlib
import msgbox.client
from logging import critical as log


if '__main__' == __name__:
    for r in msgbox.client.tail(sys.argv[1]):
        url = '{server}/{path}/{term}/{seq}'.format(**r)
        if 'ok' == r['status']:
            print('{} {} {}'.format(
                hashlib.md5(r['blob']).hexdigest(),
                len(r['blob']), url))
        else:
            log('{} {}'.format(r['status'], url))
            time.sleep(1)
