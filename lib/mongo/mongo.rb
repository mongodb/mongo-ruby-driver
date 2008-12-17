# ---
# Copyright (C) 2008 10gen Inc.
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
# +++

require 'mongo/db'

module XGen
  module Mongo
    module Driver

      # Represents a Mongo database server.
      class Mongo

        DEFAULT_PORT = 27017

        # Host default is 'localhost', port default is DEFAULT_PORT.
        def initialize(host='localhost', port=DEFAULT_PORT)
          @host, @port = host, port
        end

        # Return the XGen::Mongo::Driver::DB named +db_name+.
        def db(db_name)
          XGen::Mongo::Driver::DB.new(db_name, @host, @port)
        end

        # Not implemented.
        def clone_database(from)
          raise "not implemented"
        end

        # Not implemented.
        def copy_database(from_host, from_db, to_db)
          raise "not implemented"
        end

      end
    end
  end
end

