Use
===

This works in two passes, pass 1) creating a set of SSTables for a
keyspace, pass 2) bulk importing those SSTables.


What you need
-------------
If you need to build from source, install maven and do:

    $ mvn package

Copy `target/restbase-storage-tools-{latest}-jar-with-dependencies.jar`,
along with the `bin/krv-lint` and `bin/sstableloader` scripts to a
machine in the cluster.  Technically, any machine will do, but make
sure it has both the disk space to store the generated tables, and a
minimum of extant IO (I tend use restbase1016, but any of the Intel
SSD-equipped hosts will work).


Creating SSTables
-----------------
The `krv-lint` wrapper script will scan a table, identify candidate
rows for deletion, and write the corresponding tombstones to SSTables
in a directory.  The app itself logs to `stdout`, the wrapper script
writes this output to a log file in the directory where you invoked it
(in addition to also sending it to `stdout`).

Do everything in a screen session:

    $ screen -S krv-lint
    
Create a directory somewhere with sufficient space:

    $ sudo mkdir /srv/sda4/sstables
    
Invoke the `krv-lint` wrapper, overriding the output directory (as root if necessary):

    $ sudo OUTPUTDIR=/srv/sda4/sstables ./krv-lint wikipedia_T_parsoid__ng_html
    ...
    
Bulk-importing SSTables
-----------------------

Each time you invoke `krv-lint`, a new UUID-based directory is
created, you can find this directory by examining log output.  For
example:

    22:17:55.305 [main] INFO  o.w.r.k.Linter - SSTables written to output directory: /srv/sda4/sstables/0ac64c7f-a691-4469-88b4-0b38f53ce359/enwiki_T_parsoid__ng_html/data

The `sstableloader` wrapper script needs the full path to where the
SSTables are (it can suss out the keyspace and table name from the
path):

    $ sudo ./sstableloader /srv/sda4/sstables/0ac64c7f-a691-4469-88b4-0b38f53ce359/wikipedia_T_parsoid__ng_html/data
    $ ...
    
Be sure to cleanup afterward:

    $ rm -r /srv/sda4/sstables

