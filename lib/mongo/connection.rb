# --
# Copyright (C) 2008-2009 10gen Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
# ++

require 'mongo/db'

module Mongo

  # A connection to MongoDB.
  class Connection

    DEFAULT_PORT = 27017

    # Create a Mongo database server instance. You specify either one or a
    # pair of servers. If one, you also say if connecting to a slave is
    # OK. In either case, the host default is "localhost" and port default
    # is DEFAULT_PORT.
    #
    # If you specify a pair, pair_or_host is a hash with two keys :left
    # and :right. Each key maps to either
    # * a server name, in which case port is DEFAULT_PORT
    # * a port number, in which case server is "localhost"
    # * an array containing a server name and a port number in that order
    #
    # +options+ are passed on to each DB instance:
    #
    # :slave_ok :: Only used if one host is specified. If false, when
    #              connecting to that host/port a DB object will check to
    #              see if the server is the master. If it is not, an error
    #              is thrown.
    #
    # :auto_reconnect :: If a DB connection gets closed (for example, we
    #                    have a server pair and saw the "not master"
    #                    error, which closes the connection), then
    #                    automatically try to reconnect to the master or
    #                    to the single server we have been given. Defaults
    #                    to +false+.
    # :logger :: Optional Logger instance to which driver usage information
    #            will be logged.
    #
    # Since that's so confusing, here are a few examples:
    #
    #  Connection.new                         # localhost, DEFAULT_PORT, !slave
    #  Connection.new("localhost")            # localhost, DEFAULT_PORT, !slave
    #  Connection.new("localhost", 3000)      # localhost, 3000, slave not ok
    #  # localhost, 3000, slave ok
    #  Connection.new("localhost", 3000, :slave_ok => true)
    #  # localhost, DEFAULT_PORT, auto reconnect
    #  Connection.new(nil, nil, :auto_reconnect => true)
    #
    #  # A pair of servers. DB will always talk to the master. On socket
    #  # error or "not master" error, we will auto-reconnect to the
    #  # current master.
    #  Connection.new({:left  => ["db1.example.com", 3000],
    #             :right => "db2.example.com"}, # DEFAULT_PORT
    #            nil, :auto_reconnect => true)
    #
    #  # Here, :right is localhost/DEFAULT_PORT. No auto-reconnect.
    #  Connection.new({:left => ["db1.example.com", 3000]})
    #
    # When a DB object first connects to a pair, it will find the master
    # instance and connect to that one.
    def initialize(pair_or_host=nil, port=nil, options={})
      @pair = case pair_or_host
               when String
                 [[pair_or_host, port ? port.to_i : DEFAULT_PORT]]
               when Hash
                connections = []
                connections << pair_val_to_connection(pair_or_host[:left])
                connections << pair_val_to_connection(pair_or_host[:right])
                connections
               when nil
                 [['localhost', DEFAULT_PORT]]
               end

      @options = options
    end

    # Return the Mongo::DB named +db_name+. The slave_ok and
    # auto_reconnect options passed in via #new may be overridden here.
    # See DB#new for other options you can pass in.
    def db(db_name, options={})
      DB.new(db_name, @pair, @options.merge(options))
    end
    
    def logger
      @options[:logger]
    end

    # Returns a hash containing database names as keys and disk space for
    # each as values.
    def database_info
      doc = single_db_command('admin', :listDatabases => 1)
      h = {}
      doc['databases'].each { |db|
        h[db['name']] = db['sizeOnDisk'].to_i
      }
      h
    end

    # Returns an array of database names.
    def database_names
      database_info.keys
    end

    # Drops the database +name+.
    def drop_database(name)
      single_db_command(name, :dropDatabase => 1)
    end

    # Return the build information for the current connection.
    def server_info
      db("admin").db_command(:buildinfo => 1)
    end

    # Returns the build version of the current server, using
    # a ServerVersion object for comparability.
    def server_version
      ServerVersion.new(server_info["version"])
    end

    protected

    # Turns an array containing a host name string and a
    # port number integer into a [host, port] pair array.
    def pair_val_to_connection(a)
      case a
      when nil
        ['localhost', DEFAULT_PORT]
      when String
        [a, DEFAULT_PORT]
      when Integer
        ['localhost', a]
      when Array
        a
      end
    end

    # Send cmd (a hash, possibly ordered) to the admin database and return
    # the answer. Raises an error unless the return is "ok" (DB#ok?
    # returns +true+).
    def single_db_command(db_name, cmd)
      db = nil
      begin
        db = db(db_name)
        doc = db.db_command(cmd)
        raise "error retrieving database info: #{doc.inspect}" unless db.ok?(doc)
        doc
      ensure
        db.close if db
      end
    end
  end
end
