import configparser
import glob
import os
import sys
from datetime import datetime

from mako.template import Template
from sqlalchemy import create_engine
from sqlalchemy.sql import text

CREATE_MIGRATIONS_TABLE_STATEMENT = text("""
    set search_path = :schema;

    CREATE TABLE IF NOT EXISTS revision
    (
       "Revision" character varying(255) NOT NULL,
       CONSTRAINT revisions_pk PRIMARY KEY ("Revision")
    )
    WITH (
      OIDS = FALSE
    );
    
    CREATE TABLE IF NOT EXISTS dependent_revision
    (
       "ParentRevision" character varying(255) NOT NULL,
       "DependentRevision" character varying(255) NOT NULL,
       CONSTRAINT dependent_revisions_pk PRIMARY KEY ("ParentRevision", "DependentRevision"),
       CONSTRAINT "parent_revision_FK" FOREIGN KEY ("ParentRevision")
        REFERENCES revision ("Revision") MATCH SIMPLE
        ON UPDATE NO ACTION ON DELETE NO ACTION,
       CONSTRAINT "dependent_revision_FK" FOREIGN KEY ("DependentRevision")
        REFERENCES revision ("Revision") MATCH SIMPLE
        ON UPDATE NO ACTION ON DELETE NO ACTION
    )
    WITH (
      OIDS = FALSE
    );
""")

READ_ONLY_ACCESS_STATEMENT = """
    set search_path = {schema};

    GRANT SELECT ON TABLE revision TO GROUP {user};
    
    GRANT SELECT ON TABLE dependent_revision TO GROUP {user};
"""

INSERT_REVISION_TEMPLATE = text("""
    set search_path = :schema;

    INSERT INTO revision("Revision") VALUES (:revision_name);
""")

INSERT_DEPENDENT_REVISION_TEMPLATE = text("""
    set search_path = :schema;
    
    INSERT INTO dependent_revision("ParentRevision", "DependentRevision") VALUES (:parent, :dependent);
""")

DELETE_REVISION_TEMPLATE = text("""
    set search_path = :schema;
    
    DELETE FROM revision WHERE "Revision" = :revision_name;
""")

DELETE_DEPENDENT_REVISION_TEMPLATE = text("""
    set search_path = :schema;
    
    DELETE FROM dependent_revision where "DependentRevision" = :revision_name;
""")

FIND_PREVIOUSLY_RUN_MIGRATIONS = text("""
    set search_path = :schema;
    
    SELECT * FROM revision;
""")

FIND_DEPENDENT_REVISIONS = text("""
    set search_path = :schema;
    
    SELECT "DependentRevision" from dependent_revision WHERE "ParentRevision" = :revision_name;
""")

PREDECESSOR_MARKER = '-- #'

MIGRATION_FILENAME_TEMPLATE = '{time}_{name}'

CURRENT_DIRECTORY = os.path.dirname(os.path.realpath(__file__))


def initialize_project(connection_string, schema, read_only_users):
    print('Adding revision and dependent_revision tables to the database.')
    db_engine = create_engine(connection_string)
    with db_engine.begin() as connection:
        connection.execute(CREATE_MIGRATIONS_TABLE_STATEMENT, schema=schema)

        # the following is bad practice as buiding a statement this way could lead to sql injection, BUUUUUUT since
        # this is a tool to be run by someone that already has the password to the database this isn't much of a
        # vulnerability.  this is only necessary because user names can't get inserted into sqlalchemy text objects
        # without getting surrounded by quotes which in turn breaks the statement.
        if read_only_users:
            for user in read_only_users:
                connection.execute(READ_ONLY_ACCESS_STATEMENT.format(schema=schema, user=user))


def create_revision(name, schema):
    _create_directory('up')
    _create_directory('down')

    print('Generating template file for the requested migration.')
    template_path = CURRENT_DIRECTORY + '/revision.mako.sql'

    revision_name = MIGRATION_FILENAME_TEMPLATE.format(time=datetime.utcnow().strftime('%Y%m%d%H%M%S'),
                                                       name='_'.join(name.split()))

    migrations_directory = CURRENT_DIRECTORY + '/up/'
    all_revisions = glob.glob(migrations_directory + '*.sql')
    all_revisions.sort()
    previous_migration = '' if not all_revisions else \
        (PREDECESSOR_MARKER + ' ' + all_revisions[-1][:-4].replace(migrations_directory, ''))

    up_template = Template(filename=template_path).render(schema=schema, mode='up', previous_migration=previous_migration)
    down_template = Template(filename=template_path).render(schema=schema, mode='down')

    print('Outputting scripts...', end='')
    filename = revision_name + ".sql"
    _output_migration(CURRENT_DIRECTORY + '/up/' + filename, up_template)
    _output_migration(CURRENT_DIRECTORY + '/down/' + filename, down_template)
    print('DONE')


def apply_migrations(connection_string, schema):
    print('Finding previously applied revisions...')
    db_engine = create_engine(connection_string)
    with db_engine.connect() as connection:
        results = connection.execute(FIND_PREVIOUSLY_RUN_MIGRATIONS, schema=schema)

    applied_migrations = [revision[0] for revision in results]
    print('Found {0} previously applied revision(s).'.format(len(applied_migrations)))

    migrations_directory = CURRENT_DIRECTORY + '/up/'
    all_revisions = glob.glob(migrations_directory + '*.sql')
    revision_to_file = {filename.replace(migrations_directory, '').replace('.sql', ''):filename for filename in all_revisions}

    for applied_migration in applied_migrations:
        if applied_migration in revision_to_file:  # should always be true
            del revision_to_file[applied_migration]

    if len(revision_to_file) == 0:
        print('All revisions have been previously applied to the database.')

        return  # safe to break out here as nothing needs to be applied
    else:
        print('Found {0} revision(s) to apply.'.format(len(revision_to_file)))

    revision_to_predecessors = {}
    for revision, file in revision_to_file.items():
        revision_to_predecessors[revision] = ([], [])
        with open(file, 'r') as revision_file:
            for line in revision_file:
                if line.startswith(PREDECESSOR_MARKER):
                    predecessor_revision = line.replace(PREDECESSOR_MARKER, '').strip()
                    revision_to_predecessors[revision][0].append(predecessor_revision)
                    if predecessor_revision not in applied_migrations:
                        revision_to_predecessors[revision][1].append(predecessor_revision)

    # there should be at least one revision where all migrations have been applied otherwise there is some sort of
    # circular dependency that must be fixed outside of this tool so end processing
    revisions_with_no_outstanding_predecessors = \
        [revision for revision in revision_to_predecessors if not revision_to_predecessors[revision][1]]
    if not revisions_with_no_outstanding_predecessors and revision_to_file:
        print('Invalid dependencies found.  No revisions have had all predecessors applied.  This could indicate '
              'incorrect dependencies are listed in the migration files or the revision table has been manually '
              'modified.  No changes will be applied.')

        sys.exit(1)
    else:
        _apply_revisions_to_database(db_engine, revision_to_file, revision_to_predecessors,
                                     revisions_with_no_outstanding_predecessors, schema)


def remove_migration(revision_to_remove, connection_string, schema):
    db_engine = create_engine(connection_string)
    with db_engine.connect() as connection:
        results = connection.execute(FIND_DEPENDENT_REVISIONS, schema=schema, revision_name=revision_to_remove)

    dependent_migrations = [result[0] for result in results]
    for migration in dependent_migrations:
        remove_migration(migration, connection_string, schema)

    # either there were no dependent migrations or they've already been deleted so we can now undo this one
    file_to_apply = CURRENT_DIRECTORY + '/down/' + revision_to_remove + '.sql'
    print('Undoing revision {0}...'.format(revision_to_remove), end='')
    with open(file_to_apply) as file:
        migration_command = file.read()
        with db_engine.begin() as connection:
            connection.execute(migration_command)
            connection.execute(DELETE_DEPENDENT_REVISION_TEMPLATE, schema=schema, revision_name=revision_to_remove)
            connection.execute(DELETE_REVISION_TEMPLATE, schema=schema, revision_name=revision_to_remove)
    print('DONE')


def _apply_revisions_to_database(db_engine, revision_to_file, revision_to_predecessors,
                                 revisions_with_no_outstanding_predecessors, schema):
    all_migrations_applied = False
    while not all_migrations_applied:
        # it's safe to apply a migration to the database once all of its predecessors have been applied
        for revision in revisions_with_no_outstanding_predecessors:
            with open(revision_to_file[revision]) as file:
                migration_command = file.read()

            print('Applying revision {0}...'.format(revision), end='')
            with db_engine.begin() as connection:
                connection.execute(migration_command)
                connection.execute(INSERT_REVISION_TEMPLATE, schema=schema, revision_name=revision)
                for parent_revision in revision_to_predecessors[revision][0]:
                    connection.execute(INSERT_DEPENDENT_REVISION_TEMPLATE, schema=schema, parent=parent_revision,
                                       dependent=revision)
            print('DONE')

            del revision_to_predecessors[revision]
            for predecessors in revision_to_predecessors.values():
                if revision in predecessors[1]:
                    # predecessor has been applied so we can delete the dependency
                    predecessors[1].remove(revision)

        # all revisions without predecessors have been applied so now check the new state of the list and loop again if needed
        revisions_with_no_outstanding_predecessors = \
            [revision for revision in revision_to_predecessors if not revision_to_predecessors[revision][1]]
        all_migrations_applied = len(revisions_with_no_outstanding_predecessors) == 0


def _output_migration(filename, template):
    with open(filename, 'w') as output_file:
        output_file.write(template)


def _create_directory(directory_name):
    desired_directory = CURRENT_DIRECTORY + '/' + directory_name
    if os.path.exists(desired_directory):
        print('Not creating /{0} directory as it already exists.'.format(directory_name))
    else:
        print('Creating /{0} directory...'.format(directory_name))
        os.makedirs(desired_directory)
        print('Created /{0} directory.'.format(directory_name))


if __name__ == '__main__':
    if len(sys.argv) < 2:
        print('Usage: migrate.py <action> (<migration-name>)')

        sys.exit(1)
    else:
        connection_string_value = None
        schema_name_value = None
        application_users_value = None

        config = configparser.ConfigParser()
        if config.read(CURRENT_DIRECTORY + '/migration_config.ini'):
            if 'DEFAULT' not in config:
                print('migration_config.ini was found, but it did not contain a DEFAULT section.')

                sys.exit(1)
            else:
                default_configs = config['DEFAULT']
                if 'DatabaseConnectionString' not in default_configs or \
                        'DatabaseSchema' not in default_configs or \
                        'ApplicationUsers' not in default_configs:
                    print('migration_config.ini was found, but it did not contain the following required keys: '
                          'DatabaseConnectionString, DatabaseSchema, and ApplicationUsers.')

                    sys.exit(1)
                else:
                    connection_string_value = default_configs['DatabaseConnectionString']
                    schema_name_value = default_configs['DatabaseSchema']
                    application_users_value = [user for user in default_configs['ApplicationUsers'].split(',') if user]
        else:
            print('migration_config.ini was not found.  Create this file and populate it with the following keys: '
                  'DatabaseConnectionString, DatabaseSchema, and ApplicationUsers.')

            sys.exit(1)

    if sys.argv[1] == 'init':
        initialize_project(connection_string_value, schema_name_value, application_users_value)
    elif sys.argv[1] == 'up':
        apply_migrations(connection_string_value, schema_name_value)
    elif sys.argv[1] == 'down':
        if len(sys.argv) < 3:
            print('A revision name to undo must be provided.')

            sys.exit(1)
        else:
            remove_migration(sys.argv[2], connection_string_value, schema_name_value)
    elif sys.argv[1] == 'create':
        if len(sys.argv) < 3:
            print('A name must be provided for this migration.')

            sys.exit(1)
        else:
            create_revision(sys.argv[2], schema_name_value)
    else:
        print('Unknown action provided.  Action should be one of the following: init, up, down, create')

        sys.exit(1)
