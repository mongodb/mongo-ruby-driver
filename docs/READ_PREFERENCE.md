# Read Preference in Ruby

## About Read Preference

Read preferences determine the candidate replica set members to which a query or command can be sent. They consist of a *mode* specified as a symbol and an array of hashes known as *tag_sets*.

Read preference mode is configured by providing the read option to a connection, database, collection, or cursor.

    @collection.find({:doc => 'foo'}, :read => :primary)    # read from primary only
    @collection.find({:doc => 'foo'}, :read => :secondary)  # read from secondaries only

Used in conjunction with tag_sets:

    @collection.find({:name => 'foo'}, :read => :secondary_preferred, :tag_sets => [{:continent => 'USA'}])

*Please Note*: Behavior of some read preference modes have changed in version 1.7.0:

* `:secondary_preferred` mode is now used to prefer reads from secondary members (before this was the behavior of `:secondary`).
* `:secondary_only` mode (which only allowed reads from secondaries) is now called `:secondary`.

## Read preference inheritance

The Ruby driver allows you to set read preference on each of four levels: the connection, database, collection, and cursor (or read operation).
Objects will inherit the default read preference from their parents. Thus, if you set a read preference of `{:read => :secondary}` when creating
a new connection, then all databases and collections created from that connection will inherit the same setting. See this code example:

    @con = Mongo::ReplSetConnection.new(['localhost:27017','localhost:27018'], :read => :secondary)
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

## Modes

You can using the `:read` option to specify a query's read preference mode. There are five possible options.

### :primary

With primary, all read operations from the client will use the primary member only. This is the default read preference.

If the primary is unavailable, all operations with this preference produce an error or throw an exception. Primary read preference modes are not compatible with read preferences modes that use tag sets If you specify a tag set with primary, the driver will produce an error.

### :primary_preferred

With the primaryPreferred read preference mode, operations will read from the primary member of the set in most situations. However, if the primary is unavailable, as is the case during failover situations, then these read operations can read from secondary members.

When the read preference includes a tag set, the client will first read from the primary, if it is available, and then from secondaries that match the specified tags. If there are no secondaries with tags that match the specified tags, this read operation will produce an error.

### :secondary

With the secondary read preference mode, operations will read from the secondary member of the set if available. However, if there are no secondaries available, then these operations will produce an error or exception.

Most sets have at least one secondary, but there are situations where there may not be an available secondary. For example, a set with a primary, a secondary, and an arbiter may not have any secondaries if a member is ever in recovering mode.

When the read preference includes a tag set, the client will attempt to find a secondary members that match the specified tag set and directs reads to a random secondary from among the nearest group. If there are no secondaries with tags that match the specified tag set, this read operation will produce an error.

### :secondary_preferred

With the secondaryPreferred, operations will read from secondary members, but in situations where the set only has a primary instance, the read operation will use the set’s primary.

When secondaryPreferred reads from a secondary and the read preference includes a tag set, the client will attempt to find a secondary members that match the specified tag set and directs reads to a random secondary from among the nearest group. If there are no secondaries with tags that match the specified tag set, this read operation will produce an error.

### :nearest

With the nearest, the driver will read from the nearest member of the set according to the member selection process nearest read operations will not have any consideration for the type of the set member. Reads in nearest mode may read from both primaries and secondaries.

Set this mode when you want minimize the effect of network latency on read operations without preference for current or stale data.

If you specify a tag set, the client will attempt to find a secondary members that match the specified tag set and directs reads to a random secondary from among the nearest group.

## Tag Sets

Tag sets can be used in for data center awareness by filtering secondary read operations. Primary reads occur independent of any tags.

A member matches a tag set if its tags match all the tags in the set. For example, a member tagged "{ dc: 'ny', rack: 2, size: 'large' }" matches the tag set "{ dc: 'ny', rack: 2 }". A member's extra tags don't affect whether it's a match.

Here is an example of a query which sends read operations to members in rack 2.

    @collection.find({:name => 'foo'}, :read => :secondary_preferred, :tag_sets => [{:rack => '2'}])

Tag set keys may be symbols or strings. Tag set values should be specified using strings. The `to_s` method will be called on any values provided in the tag set.

Tag sets are used in conjunction with read preference mode. In this example, because we specified a mode of secondary_preferred, if no secondaries can be found that match the tag_set `{:rack => '2'}` then the primary will be used for the query.

If only one tag set is provided, the set can be passed as a single hash parameter iteself without the enclosing array.

    @collection.find({:name => 'foo'}, :read => :secondary_preferred, :tag_sets => {:rack => '2'})

Specifiying tag_sets for mode `:primary` is considered an error and will raise a MongoArgumentError as tag_sets do not affect selection of primary members and only primary members can be selected in that particular mode.
