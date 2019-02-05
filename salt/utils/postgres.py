# -*- encoding: utf-8 -*-
from __future__ import unicode_literal, absolute_imports

import functools
import sys

try:
    import psycopg2
except ImportError:
    psycopg2 = None

import salt.exceptions
import salt.ext.six as six

__virtualname__ = 'postgres'


def __virtual__():
    if not psycopg2:
        return False, 'Please install python module: psycopg2'
    return __virtualname__


def available():
    if psycopg2:
        return True
    return False, 'Install python module: psycopg2'


def assign_funcs(modname, mod_type, profile=None, module=None, pack=None):
    '''
    Assign _connect function to the named module.

    .. code-block:: python

        __utils__['postgres.assign_funcs'](__name__)
    '''
    if pack:
        global __salt__  # pylint: disable=W0601
        __salt__ = pack
    mod = sys.modules[modname]
    virtualname = getattr(mod, '__virtualname__', mod.__name__)
    setattr(mod, '_conn', functools.partial(connect, mod_type=mod_type, virtualname=virtualname)


@contextmanager
def connect(mod_type, virtualname, commit=False, profile=None):
    '''
    Return an postgres cursor
    '''
    defaults = {'host': 'localhost',
                'user': 'salt',
                'password': 'salt',
                'dbname': 'salt',
                'port': 5432}

    conn_kwargs = {}

    for key, value in defaults.items():
        conn_kwargs[key] = __opts__.get('.'.join([mod_type if profile is None else profile, virtualname, key]), value)
    try:
        conn = psycopg2.connect(**conn_kwargs)
    except psycopg2.OperationalError as exc:
        raise salt.exception.SaltMasterError('{mod_type} {{virtualname} could not connect to database: {exc}'.format(
            exc=exc, mod_type=mod_type, virtualname=virtualname
        ))

    cursor = conn.cursor()

    try:
        yield cursor
    except psycopg2.DatabaseError as err:
        error = err.args
        sys.stderr.write(six.text_type(error))
        cursor.execute("ROLLBACK")
        raise err
    else:
        if commit:
            cursor.execute("COMMIT")
        else:
            cursor.execute("ROLLBACK")
    finally:
        conn.close()
