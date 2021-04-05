#
# This file contains work-in-progress code to visualize WAL contents.
#
# This is the API endpoint that calls a 'zenith_wal_to_json' executable,
# which is a hacked version of pg_waldump that prints information about the
# records in JSON format. The code in js/waldump.js displays it.
#

import os
import re
from subprocess import PIPE, STDOUT, run, Popen

def fetch_wal(request, s3bucket):
    filename = request.args.get('filename')
    if not re.match("^[A-Za-z0-9_]+$", filename):
        raise Exception('invalid WAL filename: ' + filename)

    # FIXME: this downloads the WAL file to current dir. Use a temp dir? Pipe?
    s3bucket.download_file('walarchive/' + filename, filename)

    result = run("zenith_wal_to_json " + filename, stdout=PIPE, universal_newlines=True, shell=True)

    os.unlink(filename);

    return result.stdout
