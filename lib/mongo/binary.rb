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

module XGen
  module Mongo
    module Driver

      # An array of binary bytes. The only reason this exists is so that the
      # BSON encoder will know to output the Mongo BINARY type.
      class Binary < String; end

    end
  end
end

class String
  # Convert a string into a XGen::Mongo::Driver::Binary
  def to_mongo_binary
    XGen::Mongo::Driver::Binary.new(self)
  end
end
