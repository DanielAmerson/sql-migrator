% if mode=='up':
-- this is the migration to upgrade the database revision

-- add a list of revisions that must be run before this on the lines between start/end list of predecessors.  start each line with '-- #'
-- start list of predecessors
${previous_migration}
-- end list of predecessors
% else:
-- this is the migration to downgrade the database revision
% endif

set search_path = ${schema};

-- custom migration code start

-- custom migration code end
