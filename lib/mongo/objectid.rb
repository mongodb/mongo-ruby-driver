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

require 'mongo/util/uuid'

module XGen
  module Mongo
    module Driver

      class ObjectID

        # String UUID
        attr_reader :uuid

        # uuid is a string
        def initialize(uuid=nil)
          @uuid ||= UUID.new.generate
        end

        def to_s
          @uuid
        end
      end
    end
  end
end
