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

require 'mongo/util/ordered_hash'

module XGen
  module Mongo
    module Driver

      # Provide administrative database methods: those having to do with
      # profiling and validation.
      class Admin

        def initialize(db)
          @db = db
        end

        # Return the current database profiling level.
        def profiling_level
          oh = OrderedHash.new
          oh[:profile] = -1
          doc = @db.db_command(oh)
          raise "Error with profile command: #{doc.inspect}" unless @db.ok?(doc) && doc['was'].kind_of?(Numeric)
          case doc['was'].to_i
          when 0
            :off
          when 1
            :slow_only
          when 2
            :all
          else
            raise "Error: illegal profiling level value #{doc['was']}"
          end
        end

        # Set database profiling level to :off, :slow_only, or :all.
        def profiling_level=(level)
          oh = OrderedHash.new
          oh[:profile] = case level
                         when :off
                           0
                         when :slow_only
                           1
                         when :all
                           2
                         else
                           raise "Error: illegal profiling level value #{level}"
                         end
          doc = @db.db_command(oh)
          raise "Error with profile command: #{doc.inspect}" unless @db.ok?(doc)
        end

        # Return an array contining current profiling information from the
        # database.
        def profiling_info
          @db.query(DB::SYSTEM_PROFILE_COLLECTION, Query.new({})).to_a
        end

        # Validate a named collection.
        def validate_collection(name)
        end

      end
    end
  end
end
