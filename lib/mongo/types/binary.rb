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

require 'mongo/util/byte_buffer'

module XGen
  module Mongo
    module Driver

      # An array of binary bytes with a Mongo subtype value.
      class Binary < ByteBuffer

        SUBTYPE_BYTES = 0x02
        SUBTYPE_UUID = 0x03
        SUBTYPE_MD5 = 0x05
        SUBTYPE_USER_DEFINED = 0x80

        # One of the SUBTYPE_* constants. Default is SUBTYPE_BYTES.
        attr_accessor :subtype

        def initialize(initial_data=[], subtype=SUBTYPE_BYTES)
          super(initial_data)
          @subtype = subtype
        end

      end
    end
  end
end
