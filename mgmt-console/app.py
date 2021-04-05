from flask import request
from flask_basicauth import BasicAuth
from flask import render_template
from subprocess import PIPE, STDOUT, run, Popen
import html
import os
import re
import shutil
import logging
import time

import boto3
from boto3.session import Session
from botocore.client import Config
from botocore.handlers import set_list_objects_encoding_type_url

from flask import Flask

import waldump


app = Flask(__name__)

app.config['BASIC_AUTH_USERNAME'] = 'zenith'
app.config['BASIC_AUTH_PASSWORD'] = os.getenv('BASIC_AUTH_PASSWORD')
app.config['BASIC_AUTH_FORCE'] = True

basic_auth = BasicAuth(app)

# S3 configuration:

ENDPOINT = os.getenv('S3_ENDPOINT', 'https://localhost:9000')
ACCESS_KEY = os.getenv('S3_ACCESSKEY', 'minioadmin')
SECRET = os.getenv('S3_SECRET', '')
BUCKET = os.getenv('S3_BUCKET', 'foobucket')

print("Using bucket at " + ENDPOINT);

#boto3.set_stream_logger('botocore', logging.DEBUG)

session = Session(aws_access_key_id=ACCESS_KEY,
                  aws_secret_access_key=SECRET,
                  region_name=os.getenv('S3_REGION', 'auto'))

# needed for google cloud?
session.events.unregister('before-parameter-build.s3.ListObjects',
                          set_list_objects_encoding_type_url)

s3resource = session.resource('s3',
                              endpoint_url=ENDPOINT,
                              verify=False,
                              config=Config(signature_version='s3v4'))
s3bucket = s3resource.Bucket(BUCKET)

s3_client = boto3.client('s3',
                         endpoint_url=ENDPOINT,
                         verify=False,
                         config=Config(signature_version='s3v4'),
                         aws_access_key_id=ACCESS_KEY,
                         aws_secret_access_key=SECRET)


@app.route("/")
def index():
    return render_template("index.html")


@app.route("/api/waldump")
def render_waldump():
    return render_template("waldump.html")

@app.route('/api/fetch_wal')
def fetch_wal():
    return waldump.fetch_wal(request, s3bucket);

@app.route("/api/server_status")
def server_status():
    dirs = os.listdir("pgdatadirs")
    dirs.sort()

    primary = None
    standbys = []

    for dirname in dirs:
        
        result = run("pg_ctl status -D pgdatadirs/" + dirname, stdout=PIPE, stderr=STDOUT, universal_newlines=True, shell=True)

        srv = {
            'datadir': dirname,
            'status': result.stdout,
            'port': None
        }

        if dirname == 'primary':
            primary = srv;
            primary['port'] = 5432;
        else:
            standby_match = re.search('standby_([0-9]+)', dirname)
            if standby_match:
                srv['port'] = int(standby_match.group(1))

            standbys.append(srv);

    return {'primary': primary, 'standbys': standbys}

@app.route('/api/list_bucket')
def list_bucket():

    response = 'cloud bucket contents:<br>\n'

    for file in s3bucket.objects.all():
        response = response + html.escape(file.key) + '<br>\n'

    return response

def walpos_str(walpos):
    return '{:X}/{:X}'.format(walpos >> 32, walpos & 0xFFFFFFFF)

@app.route('/api/bucket_summary')
def bucket_summary():

    nonrelimages = []
    minwal = int(0)
    maxwal = int(0)
    minseqwal = int(0)
    maxseqwal = int(0)

    for file in s3bucket.objects.all():
        path = file.key
        match = re.search('nonreldata/nonrel_([0-9A-F]+).tar', path)
        if match:
            walpos = int(match.group(1), 16)
            nonrelimages.append(walpos_str(walpos))

        match = re.search('nonreldata/nonrel_([0-9A-F]+)-([0-9A-F]+)', path)
        if match:
            endwal = int(match.group(2), 16)
            if endwal > maxwal:
                maxwal = endwal

        match = re.search('walarchive/([0-9A-F]{8})([0-9A-F]{8})([0-9A-F]{8})', path)
        if match:
            tli = int(match.group(1), 16)
            logno = int(match.group(2), 16)
            segno = int(match.group(3), 16)
            # FIXME: this assumes default 16 MB wal segment size
            logsegno = logno * (0x100000000 / (16*1024*1024)) + segno

            seqwal = int((logsegno + 1) * (16*1024*1024))

            if seqwal > maxseqwal:
                maxseqwal = seqwal;
            if minseqwal == 0 or seqwal < minseqwal:
                minseqwal = seqwal;

    return {
        'nonrelimages': nonrelimages,
        'minwal': walpos_str(minwal),
        'maxwal': walpos_str(maxwal),
        'minseqwal': walpos_str(minseqwal),
        'maxseqwal': walpos_str(maxseqwal)
        }

def print_cmd_result(cmd_result):
    return print_cmd_result_ex(cmd_result.args, cmd_result.returncode, cmd_result.stdout)

def print_cmd_result_ex(cmd, returncode, stdout):
    res = ''
    res += 'ran command:\n' + str(cmd) + '\n'
    res += 'It returned code ' + str(returncode) + '\n'
    res += '\n'
    res += 'stdout/stderr:\n'
    res += stdout

    return res

@app.route('/api/init_primary', methods=['GET', 'POST'])
def init_primary():
    
    initdb_result = run("initdb -D pgdatadirs/primary --username=zenith --pwfile=pg-password.txt", stdout=PIPE, stderr=STDOUT, universal_newlines=True, shell=True)
    if initdb_result.returncode != 0:
        return print_cmd_result(initdb_result)
    
    # Append archive_mode and archive_command and port to postgresql.conf
    f=open("pgdatadirs/primary/postgresql.conf", "a+")
    f.write("listen_addresses='*'\n")
    f.write("archive_mode=on\n")
    f.write("archive_command='zenith_push --archive-wal-path=%p --archive-wal-fname=%f'\n")
    f.write("ssl=on\n")
    f.close()

    f=open("pgdatadirs/primary/pg_hba.conf", "a+")
    f.write("# allow SSL connections with password from anywhere\n")
    f.write("hostssl    all             all             0.0.0.0/0           md5\n")
    f.write("hostssl    all             all             ::0/0               md5\n")
    f.close()

    shutil.copyfile("server.crt", "pgdatadirs/primary/server.crt")
    shutil.copyfile("server.key", "pgdatadirs/primary/server.key")
    os.chmod("pgdatadirs/primary/server.key", 0o0600)
    
    start_proc = Popen(args=["pg_ctl", "start", "-D", "pgdatadirs/primary", "-l", "pgdatadirs/primary/log"], stdout=PIPE, stderr=STDOUT, universal_newlines=True, shell=False, start_new_session=True, close_fds=True)
    start_rc = start_proc.wait()
    start_stdout, start_stderr = start_proc.communicate()

    responsestr = print_cmd_result(initdb_result) + '\n'
    responsestr += print_cmd_result_ex(start_proc.args, start_rc, start_stdout)

    return responsestr

@app.route('/api/zenith_push', methods=['GET', 'POST'])
def zenith_push():
    # Stop the primary if it's running
    stop_result = run(args=["pg_ctl", "stop", "-D", "pgdatadirs/primary"], stdout=PIPE, stderr=STDOUT, universal_newlines=True, shell=False, start_new_session=True, close_fds=True)
    
    # Call zenith_push
    push_result = run("zenith_push -D pgdatadirs/primary", stdout=PIPE, stderr=STDOUT, universal_newlines=True, shell=True)

    # Restart the primary
    start_proc = Popen(args=["pg_ctl", "start", "-D", "pgdatadirs/primary", "-l", "pgdatadirs/primary/log"], stdout=PIPE, stderr=STDOUT, universal_newlines=True, shell=False, start_new_session=True, close_fds=True)
    start_rc = start_proc.wait()
    start_stdout, start_stderr = start_proc.communicate()
    
    responsestr = print_cmd_result(stop_result) + '\n'
    responsestr += print_cmd_result(push_result) + '\n'
    responsestr += print_cmd_result_ex(start_proc.args, start_rc, start_stdout) + '\n'

    return responsestr

@app.route('/api/create_standby', methods=['GET', 'POST'])
def create_standby():

    walpos = request.form.get('walpos')
    if not walpos:
        return 'no walpos'
    
    dirs = os.listdir("pgdatadirs")

    last_port = 5432

    for dirname in dirs:

        standby_match = re.search('standby_([0-9]+)', dirname)
        if standby_match:
            port = int(standby_match.group(1))
            if port > last_port:
                last_port = port

    standby_port = last_port + 1

    standby_dir = "pgdatadirs/standby_" + str(standby_port)

    # Call zenith_restore
    restore_result = run(["zenith_restore", "--end=" + walpos, "-D", standby_dir], stdout=PIPE, stderr=STDOUT, encoding='latin1')
    responsestr = print_cmd_result(restore_result)

    if restore_result.returncode == 0:
        # Append hot_standby and port to postgresql.conf
        f=open(standby_dir + "/postgresql.conf", "a+")
        f.write("hot_standby=on\n")
        f.write("port=" + str(standby_port) + "\n")
        f.close()

        start_proc = Popen(args=["pg_ctl", "start", "-D", standby_dir, "-l", standby_dir + "/log"], stdout=PIPE, stderr=STDOUT, universal_newlines=True, shell=False, start_new_session=True, close_fds=True)
        start_rc = start_proc.wait()
        start_stdout, start_stderr = start_proc.communicate()
        responsestr += '\n\n' + print_cmd_result_ex(start_proc.args, start_rc, start_stdout)

    return responsestr

@app.route('/api/destroy_server', methods=['GET', 'POST'])
def destroy_primary():

    datadir = request.form.get('datadir')

    # Check that the datadir parameter doesn't contain anything funny.
    if not re.match("^[A-Za-z0-9_-]+$", datadir):
        raise Exception('invalid datadir: ' + datadir)
    
    # Stop the server if it's running
    stop_result = run(args=["pg_ctl", "stop", "-m", "immediate", "-D", "pgdatadirs/" + datadir], stdout=PIPE, stderr=STDOUT, universal_newlines=True, shell=False, start_new_session=True, close_fds=True)

    shutil.rmtree('pgdatadirs/' + datadir, ignore_errors=True)

    responsestr = print_cmd_result(stop_result) + '\n'
    responsestr += 'Deleted datadir ' + datadir + '.\n'

    return responsestr

@app.route('/api/restore_primary', methods=['GET', 'POST'])
def restore_primary():

    # Call zenith_restore
    restore_result = run(["zenith_restore", "-D", "pgdatadirs/primary"], stdout=PIPE, stderr=STDOUT, encoding='latin1')
    responsestr = print_cmd_result(restore_result)

    # Append restore_command to postgresql.conf, so that it can find the last raw WAL segments
    f=open("pgdatadirs/primary/postgresql.conf", "a+")
    f.write("listen_addresses='*'\n")
    f.write("restore_command='zenith_restore --archive-wal-path=%p --archive-wal-fname=%f'\n")
    f.write("ssl=on\n")
    f.close()
    
    if restore_result.returncode == 0:
        start_proc = Popen(args=["pg_ctl", "start", "-D", "pgdatadirs/primary", "-l", "pgdatadirs/primary/log"], stdout=PIPE, stderr=STDOUT, universal_newlines=True, shell=False, start_new_session=True, close_fds=True)
        start_rc = start_proc.wait()
        start_stdout, start_stderr = start_proc.communicate()
        responsestr += print_cmd_result_ex(start_proc.args, start_rc, start_stdout)

    return responsestr

@app.route('/api/slicedice', methods=['GET', 'POST'])
def run_slicedice():
    result = run("zenith_slicedice", stdout=PIPE, stderr=STDOUT, universal_newlines=True, shell=True)
    
    responsestr = print_cmd_result(result)

    return responsestr

@app.route('/api/reset_demo', methods=['POST'])
def reset_all():
    result = run("pkill -9 postgres", stdout=PIPE, stderr=STDOUT, universal_newlines=True, shell=True)

    dirs = os.listdir("pgdatadirs")
    for dirname in dirs:
        shutil.rmtree('pgdatadirs/' + dirname)
        
    for file in s3bucket.objects.all():
        s3_client.delete_object(Bucket = BUCKET, Key = file.key)

    responsestr = print_cmd_result(result) + '\n'
    responsestr += '''
Deleted all Postgres datadirs.
Deleted all files in object storage bucket.
'''

    return responsestr

if __name__ == '__main__':
    app.run()
