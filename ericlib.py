# vim: set expandtab ts=4 sw=4 filetype=python fileencoding=utf8:

import logging
import textwrap

import psycopg2.extras

log = logging.getLogger(__name__)

fighters_and_moves = dict({

    "Liu Kang": [
        ("Fire blast", 20),
        ("Flying kick", 30),
    ],

    'Scorpion': [
        ("Harpoon", 30),
        ("Teleport punch", 30),
    ],

    'Sub Zero': [
        ("Ice ball", 10),
        ("Foot slide", 20),
    ]

})

def create_tables(pgconn):

    cursor = pgconn.cursor()

    cursor.execute(textwrap.dedent("""
        create table fighters
        (
            fighter_id serial primary key,
            title text not null unique
        )
        """))

    cursor.execute(textwrap.dedent("""
        create table special_moves
        (
            special_move_id serial primary key,
            fighter_id integer references fighters (fighter_id),
            title text not null,
            unique(fighter_id, title),
            damage integer not null
        )
        """))


def insert_data(pgconn):

    global fighters_and_moves

    cursor = pgconn.cursor()

    for fighter, special_moves in fighters_and_moves.items():

        # I discovered the "returning" keyword a few years ago and it
        # has vastly simplified a lot of my database code.

        cursor.execute(textwrap.dedent("""
            insert into fighters
            (title)
            values
            (%(fighter)s)
            returning fighter_id
            """), {
                'fighter': fighter,
            })

        fighter_id = cursor.fetchone().fighter_id

        log.info("Stored fighter {0} with fighter ID {1}.".format(
            fighter, fighter_id))

        for title, damage in special_moves:

            cursor.execute(textwrap.dedent("""
                insert into special_moves
                (fighter_id, title, damage)
                values
                (
                %(fighter_id)s, %(title)s, %(damage)s
                )
                returning special_move_id
                """), {
                    'fighter_id': fighter_id,
                    'title': title,
                    'damage': damage
                })

            special_move_id = cursor.fetchone().special_move_id

            log.info(
                "Stored special move {0} with special move "
                "ID {1}.".format(
                    title, special_move_id))


def do_ugly_report(pgconn):

    cursor = pgconn.cursor()

    cursor.execute(textwrap.dedent("""
        select f.title, sm.title as move_name, sm.damage

        from fighters f
        join special_moves sm
        on f.fighter_id = sm.fighter_id

        order by f.title, sm.title
        """))

    print("+" * 60)
    print("UGLY REPORT")
    print("+" * 60)
    print("")

    print("{0:20} {1:20} {2:20}".format(
        "Fighter",
        "Move",
        "Damage"))

    print("-"*20 + " " + "-"*20 + " " + "-"*18 + " ")

    for row in cursor:

        # the < symbol means "left-justify", which seems to be the
        # default for strings, but not for integers.
        print("{0:20} {1:20} {2:<20}".format(*row))

    print("")

def do_pretty_report_with_array_agg_1(pgconn):

    """
    Use array_agg to roll up the just the titles of the special moves.

    The titles will come back as a list of strings.
    """

    cursor = pgconn.cursor()

    cursor.execute(textwrap.dedent("""
        select f.title,
        array_agg(sm.title order by sm.title) as special_moves

        from fighters f
        join special_moves sm
        on f.fighter_id = sm.fighter_id

        group by f.fighter_id

        order by f.title
        """))

    print("+" * 60)
    print("PRETTY REPORT 1")
    print("+" * 60)
    print("")

    for row in cursor:

        print("{0}".format(row.title))

        log.debug("In array_agg_1, type(row.special_moves) is {0}".format(
            type(row.special_moves)))

        for move in row.special_moves:

            print("    {0}".format(move))

        print("")


def do_pretty_report_with_array_agg_2(pgconn):

    """
    Try to use array_agg to roll up more than one columns from the
    special moves table.

    But beware!  This doesn't work right!  Because psycopg2 has no
    understanding of how to convert the thing made with the row(...)
    function, it just returns it as a string.

    """

    cursor = pgconn.cursor()

    cursor.execute(textwrap.dedent("""
        select f.title,

        array_agg(row(sm.title, sm.damage) order by sm.title) as special_moves

        from fighters f
        join special_moves sm
        on f.fighter_id = sm.fighter_id

        group by f.fighter_id

        order by f.title
        """))

    print("+" * 60)
    print("PRETTY REPORT 2")
    print("+" * 60)
    print("")

    for row in cursor:

        print("{0}".format(row.title))

        log.debug("In array_agg_2, type(row.special_moves) is {0}".format(
            type(row.special_moves)))

        print("    {0}".format(row.special_moves))

        print("")

def register_type(pgconn):

    # Tell psycopg2 that when it gets an instance of a special moves
    # type, it should make a named tuple instance for the special moves
    # table.
    psycopg2.extras.register_composite('special_moves', pgconn)


def do_pretty_report_with_array_agg_3(pgconn):

    """
    Note this weird syntax in the SQL::

        row(
            sm.special_move_id,
            sm.fighter_id,
            sm.title,
            sm.damage)::special_moves

    That :: operator casts whatever is on the left to a different type.

    Every table in postgresql is also a user-defined type.

    In this case, I'm saying "treat these four values in this row as a
    special_moves type instance.

    Here are much simpler / less scary examples::

        >>> cursor.execute("select '2014-08-01' as x")
        >>> type(cursor.fetchone().x)
        <type 'str'>

        >>> cursor.execute("select '2014-08-01'::date as x")
        >>> type(cursor.fetchone().x)
        <type 'datetime.date'>

    """


    cursor = pgconn.cursor()

    cursor.execute(textwrap.dedent("""
        select f.title,
        array_agg(row(
            sm.special_move_id,
            sm.fighter_id,
            sm.title,
            sm.damage)::special_moves) as special_moves

        from fighters f
        join special_moves sm
        on f.fighter_id = sm.fighter_id

        group by f.fighter_id

        order by f.title
        """))

    print("+" * 60)
    print("PRETTY REPORT 3")
    print("+" * 60)
    print("")

    for row in cursor:

        print("{0}".format(row.title))

        log.debug("In array_agg_3, type(row.special_moves) is {0}".format(
            type(row.special_moves)))

        for sm in row.special_moves:

            log.debug("type(sm) is {0}.".format(type(sm)))

            print("    {0:20} {1:<20}".format(sm.title, sm.damage))

        print("")


class Fighter(object):

    def __init__(self, fighter_id, title):
        self.fighter_id = fighter_id
        self.title = title

    def __str__(self):

        return "{0}: {1} (fighter ID: {2})".format(
            self.__class__.__name__,
            self.title,
            self.fighter_id)

class FighterFactory(psycopg2.extras.CompositeCaster):

    def make(self, values):
        d = dict(zip(self.attnames, values))
        return Fighter(**d)

class SpecialMove(object):

    def __init__(self, special_move_id, fighter_id, title,  damage):
        self.special_move_id = special_move_id
        self.fighter_id = fighter_id
        self.title = title
        self.damage = damage

    def __str__(self):

        return "{0}: {1} (special move ID: {2})".format(
            self.__class__.__name__,
            self.title,
            self.special_move_id)

class SpecialMoveFactory(psycopg2.extras.CompositeCaster):

    def make(self, values):
        d = dict(zip(self.attnames, values))
        return SpecialMove(**d)

def register_home_made_types(pgconn):

    # Register our own home-made FighterFactory to instantiate instances
    # of our own Fighter class.
    psycopg2.extras.register_composite(
        'fighters',
        pgconn,
        factory=FighterFactory)

    # Same thing, but different tables.
    psycopg2.extras.register_composite(
        'special_moves',
        pgconn,
        factory=SpecialMoveFactory)

def cast_to_our_own_classes(pgconn):

    """
    In this one, I use this syntax::

        (f.*)::fighters


    instead of something like::

        row(f.fighter_id,
            f.title)::fighters

    They do the exact same thing.
    """


    cursor = pgconn.cursor()

    cursor.execute(textwrap.dedent("""
        select (f.*)::fighters as fighter

        from fighters f

        order by f.title
        """))

    log.debug("cast to our own Fighter class")

    for row in cursor:
        log.debug(row.fighter)


def do_pretty_report_with_array_agg_4(pgconn):

    """
    Get a single row back for each fighter, with two columns in each
    row.

    The first column should hold an instance of our homemade Fighter
    class.

    The second column should hold an array of instances of our
    customized
    SpecialMove class.

    """

    cursor = pgconn.cursor()

    cursor.execute(textwrap.dedent("""
        select (f.*)::fighters as fighter,
        array_agg((sm.*)::special_moves order by sm.title) as special_moves

        from fighters f

        join special_moves sm
        on f.fighter_id = sm.fighter_id

        group by f.fighter_id

        order by f.title

        """))

    print("+" * 60)
    print("PRETTY REPORT 4")
    print("+" * 60)
    print("")

    for row in cursor:

        log.debug("type(row.fighter): {0}".format(type(row.fighter)))

        # This uses the Fighter.__str__ method
        print(row.fighter)

        log.debug("type(row.special_moves): {0}".format(type(row.special_moves)))

        for sm in row.special_moves:
            log.debug("type(sm): {0}".format(type(sm)))
            print(sm)
