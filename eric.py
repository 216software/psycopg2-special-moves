# vim: set expandtab ts=4 sw=4 filetype=python fileencoding=utf8:

"""
Hi Eric:

You'll need to update the database, user, and password stuff at the
bottom of this file.  Look for the comment.

I'm not committing the transaction, so when this script finishes,
postgresql will roll everything back.  So you should be able to run this
script a bunch of times.

You can run the program like this to ONLY see the logging messages::

    $ python eric.py > /dev/null

and then like this to only see the print statements::

    $ python eric.py 2> /dev/null

"""

import logging

import psycopg2
import psycopg2.extras

import ericlib

log = logging.getLogger('eric')

if __name__ == '__main__':

    logging.basicConfig(level=logging.DEBUG)

    # Eric: you'll need to either udpate these credentials or create a
    # database and a user to match.
    pgconn = psycopg2.connect(
        "dbname=eric user=eric host=localhost password=3r1c",
        connection_factory=psycopg2.extras.NamedTupleConnection)

    ericlib.create_tables(pgconn)

    ericlib.insert_data(pgconn)

    ericlib.do_ugly_report(pgconn)

    ericlib.do_pretty_report_with_array_agg_1(pgconn)

    ericlib.do_pretty_report_with_array_agg_2(pgconn)

    ericlib.register_type(pgconn)

    ericlib.do_pretty_report_with_array_agg_3(pgconn)

    ericlib.register_home_made_types(pgconn)

    ericlib.cast_to_our_own_classes(pgconn)

    ericlib.do_pretty_report_with_array_agg_4(pgconn)

    log.info("all done!")

