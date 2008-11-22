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

require 'mongo/db'

module XGen
  module Mongo
    module Driver

      class Mongo

        DEFAULT_PORT = 27017

        def initialize(host='localhost', port=DEFAULT_PORT)
          @host, @port = host, port
        end

        def db(db_name)
          XGen::Mongo::Driver::DB.new(db_name, @host, @port)
        end

        def clone_database(from)
          raise "not implemented"
        end

        def copy_database(from_host, from_db, to_db)
          raise "not implemented"
        end

      end
    end
  end
end

