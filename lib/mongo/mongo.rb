# --
# Copyright (C) 2008-2009 10gen Inc.
#
# This program is free software: you can redistribute it and/or modify it
# under the terms of the GNU Affero General Public License, version 3, as
# published by the Free Software Foundation.
#
# This program is distributed in the hope that it will be useful, but WITHOUT
# ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
# FITNESS FOR A PARTICULAR PURPOSE. See the GNU Affero General Public License
# for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program. If not, see <http://www.gnu.org/licenses/>.
# ++

require 'mongo/db'

module XGen
  module Mongo
    module Driver

      # Represents a Mongo database server.
      class Mongo

        DEFAULT_PORT = 27017

        # Either nodes_or_host is a host name string and port is an optional
        # port number that defaults to DEFAULT_PORT, or nodes_or_host is an
        # array of arrays, where each is a host/port pair (or a host with no
        # port). Finally, if nodes_or_host is nil then host is 'localhost' and
        # port is DEFAULT_PORT. Since that's so confusing, here are a few
        # examples:
        #
        #  Mongo.new                         # localhost, DEFAULT_PORT
        #  Mongo.new("localhost")            # localhost, DEFAULT_PORT
        #  Mongo.new("localhost", 3000)      # localhost, 3000
        #  Mongo.new([["localhost"]])        # localhost, DEFAULT_PORT
        #  Mongo.new([["localhost", 3000]])  # localhost, 3000
        #  Mongo.new([["db1.example.com", 3000], ["db2.example.com", 3000]]])
        #
        # When a DB object first connects, it tries nodes and stops at the
        # first one it connects to.
        def initialize(nodes_or_host=nil, port=nil)
          @nodes = case nodes_or_host
                   when String
                     [[nodes_or_host, port || DEFAULT_PORT]]
                   when Array
                     nodes_or_host.collect { |nh| [nh[0], nh[1] || DEFAULT_PORT] }
                   when nil
                     [['localhost', DEFAULT_PORT]]
                   end
        end

        # Return the XGen::Mongo::Driver::DB named +db_name+. See DB#new for
        # +options+.
        def db(db_name, options={})
          XGen::Mongo::Driver::DB.new(db_name, @nodes, options)
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

        # Not implemented.
        def clone_database(from)
          raise "not implemented"
        end

        # Not implemented.
        def copy_database(from_host, from_db, to_db)
          raise "not implemented"
        end

        # Drops the database +name+.
        def drop_database(name)
          single_db_command(name, :dropDatabase => 1)
        end

        protected

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
  end
end

