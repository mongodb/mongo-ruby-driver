# Replica Sets in Ruby

Here follow a few considerations for those using the MongoDB Ruby driver with [replica sets](http://www.mongodb.org/display/DOCS/Replica+Sets).

### Setup

First, make sure that you've configured and initialized a replica set.

Use `Connection.multi` to connect to a replica set:

    @connection = Connection.multi([['n1.mydb.net', 27017], ['n2.mydb.net', 27017], ['n3.mydb.net', 27017]])

The driver will attempt to connect to a master node and, when found, will replace all seed nodes with known members of the replica set.

### Read slaves

If you want to read from a seconday node, you can pass :read_secondary => true to Connection#multi.

    @connection = Connection.multi([['n1.mydb.net', 27017], ['n2.mydb.net', 27017], ['n3.mydb.net', 27017]],
                  :read_secondary => true)

A random secondary will be chosen to be read from. In a typical multi-process Ruby application, you'll have a good distribution of reads across secondary nodes.

### Connection Failures

Imagine that either the master node or one of the read nodes goes offline. How will the driver respond?

If any read operation fails, the driver will raise a *ConnectionFailure* exception. It then becomes the client's responsibility to decide how to handle this.

If the client decides to retry, it's not guaranteed that another member of the replica set will have been promoted to master right away, so it's still possible that the driver will raise another *ConnectionFailure*. However, once a member has been promoted to master, typically within a few seconds, subsequent operations will succeed.

The driver will essentially cycle through all known seed addresses until a node identifies itself as master.

### Recovery

Driver users may wish to wrap their database calls with failure recovery code. Here's one possibility, which will attempt to connection
every half second and time out after thirty seconds.

    # Ensure retry upon failure
    def rescue_connection_failure(max_retries=60)
        success = false
        retries = 0
        while !success
          begin
            yield
            success = true
          rescue Mongo::ConnectionFailure => ex
            retries += 1
            raise ex if retries >= max_retries
            sleep(0.5)
          end
        end
      end
    end

    # Wrapping a call to #count()
    rescue_connection_failure do
      @db.collection('users').count()
    end

Of course, the proper way to handle connection failures will always depend on the individual application. We encourage object-mapper and application developers to publish any promising results.

### Testing

The Ruby driver (>= 1.0.6) includes some unit tests for verifying replica set behavior. They reside in *tests/replica_sets*. You can run them individually with the following rake tasks:

    rake test:replica_set_count
    rake test:replica_set_insert
    rake test:pooled_replica_set_insert
    rake test:replica_set_query

Make sure you have a replica set running on localhost before trying to run these tests.

### Further Reading

* [Replica Sets](http://www.mongodb.org/display/DOCS/Replica+Set+Configuration)
* [Replics Set Configuration](http://www.mongodb.org/display/DOCS/Replica+Set+Configuration)
