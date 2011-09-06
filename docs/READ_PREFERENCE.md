# Read Preference in Ruby

## Setting the read preference

You can using the `:read` option to specify a query's read preference. There are for now two possible options:

    @collection.find({:doc => 'foo'}, :read => :primary)
    @collection.find({:doc => 'foo'}, :read => :secondary)

In the first case, the query will be directed to the primary node in a replica set. In the second, the query will be sent
to a secondary node. The driver will attempt to choose a secondary node that's nearby, as determined by ping time. If more
than one secondary node is closeby (e.g, responds to pings within 10ms), then a random node within this subset will be chosen.

## Read preference inheritance

The Ruby driver allows you to set read preference on each of four levels: the connection, database, collection, and cursor (or read operation).
Objects will inherit the default read preference from their parents. Thus, if you set a read preference of `{:read => :secondary}` when creating
a new connection, then all databases and collections created from that connection will inherit the same setting. See this code example:

    @con = Mongo::ReplSetConnection.new([['localhost', 27017], ['localhost', 27018]], :read => :secondary)
    @db  = @con['test']
    @collection = @db['foo']
    @collection.find({:name => 'foo'})

    @collection.find({:name => 'bar'}, :read => :primary)

Here, the first call to Collection#find will use the inherited read preference, `{:read => :secondary}`. But the second call
to Collection#find overrides this setting by setting the preference to `:primary`.

You can examine the read preference on any object by calling its `read_preference` method:

    @con.read_preference
    @db.read_preference
    @collection.read_preference

## Future work

In the v2.0 release of the driver, you'll also be able to specify a read preference consisting of a set of tags. This way,
you'll be able to direct reads to a replica set member. You can follow this issue's progress here: (https://jira.mongodb.org/browse/RUBY-326).
