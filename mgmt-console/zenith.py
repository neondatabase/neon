#zenith.py
import click
import testgres
import os

from testgres import PostgresNode
from tabulate import tabulate

zenith_base_dir = '/home/anastasia/zenith/basedir'

@click.group()
def main():
    """Run the Zenith CLI."""

@click.group()
def pg():
    """Db operations

        NOTE: 'database' here means one postgresql node
    """

@click.command(name='create')
@click.option('--name', required=True)
@click.option('-s', '--storage-name', help='Name of the storage',
                                 default='zenith-local',
                                 show_default=True)
@click.option('--snapshot', help='init from the snapshot. Snap is a name or URL')
@click.option('--no-start', is_flag=True, help='Do not start created node',
                            default=False, show_default=True)
def pg_create(name, storage_name, snapshot, no_start):
    """Initialize the database"""
    node = PostgresNode()
    base_dir = os.path.join(zenith_base_dir, 'pg', name)
    node = testgres.get_new_node(name, base_dir=base_dir)
    # TODO skip init, instead of that link node with storage or upload it from snapshot
    node.init()
    if(no_start==False):
        node.start()

@click.command(name='start')
@click.option('--name', required=True)
@click.option('--snapshot')
@click.option('--read-only', is_flag=True, help='Start read-only node', show_default=True)
def pg_start(name, snapshot, read_only):
    """Start the database"""
    node = PostgresNode()
    base_dir = os.path.join(zenith_base_dir, 'pg', name)
    node = testgres.get_new_node(name, base_dir=base_dir)
    # TODO pass snapshot as a parameter
    node.start()

@click.command(name='stop')
@click.option('--name', required=True)
def pg_stop(name):
    """Stop the database"""
    node = PostgresNode()
    base_dir = os.path.join(zenith_base_dir, 'pg', name)
    node = testgres.get_new_node(name, base_dir=base_dir)
    node.stop()

@click.command(name='destroy')
@click.option('--name', required=True)
def pg_destroy(name):
    """Drop the database"""
    node = PostgresNode()
    base_dir = os.path.join(zenith_base_dir, 'pg', name)
    node = testgres.get_new_node(name, base_dir=base_dir)
    node.cleanup()

@click.command(name='list')
def pg_list():
    """List existing databases"""
    dirs = os.listdir(os.path.join(zenith_base_dir, 'pg'))
    path={}
    status={}
    data=[]

    for dirname in dirs:
        path[dirname] = os.path.join(zenith_base_dir, 'pg', dirname)
        fname = os.path.join( path[dirname], 'data/postmaster.pid')
        try:
            f = open(fname,'r')
            status[dirname] = f.readlines()[-1]
        except OSError as err:
            status[dirname]='inactive'
        data.append([dirname , status[dirname], path[dirname]])

    print(tabulate(data, headers=['Name', 'Status', 'Path']))

pg.add_command(pg_create)
pg.add_command(pg_destroy)
pg.add_command(pg_start)   
pg.add_command(pg_stop)   
pg.add_command(pg_list)



@click.group()
def storage():
    """Storage operations"""

@click.command(name='attach')
@click.option('--name')
def storage_attach(name):
    """Attach the storage"""

@click.command(name='detach')
@click.option('--name')
@click.option('--force', is_flag=True, show_default=True)
def storage_detach(name):
    """Detach the storage"""

@click.command(name='list')
def storage_list():
    """List existing storages"""

storage.add_command(storage_attach)
storage.add_command(storage_detach)
storage.add_command(storage_list)

@click.group()
def snapshot():
    """Snapshot operations"""

@click.command(name='create')
def snapshot_create():
    """Create new snapshot"""

@click.command(name='destroy')
def snapshot_destroy():
    """Destroy the snapshot"""

@click.command(name='pull')
def snapshot_pull():
    """Pull remote snapshot"""

@click.command(name='push')
def snapshot_push():
    """Push snapshot to remote"""

@click.command(name='import')
def snapshot_import():
    """Convert given format to zenith snapshot"""

@click.command(name='export')
def snapshot_export():
    """Convert zenith snapshot to PostgreSQL compatible format"""

snapshot.add_command(snapshot_create)
snapshot.add_command(snapshot_destroy)
snapshot.add_command(snapshot_pull)
snapshot.add_command(snapshot_push)
snapshot.add_command(snapshot_import)
snapshot.add_command(snapshot_export)

@click.group()
def wal():
    """WAL operations"""

@click.command()
def wallist(name="list"):
    """List WAL files"""

wal.add_command(wallist)


@click.command()
def console():
    """Open web console"""

main.add_command(pg)
main.add_command(storage)
main.add_command(snapshot)
main.add_command(wal)
main.add_command(console)


if __name__ == '__main__':
    main()
