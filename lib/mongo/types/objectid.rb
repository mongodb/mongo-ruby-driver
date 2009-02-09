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

require 'mutex_m'
require 'mongo/util/byte_buffer'

module XGen
  module Mongo
    module Driver

      # Implementation of the Babble OID. Object ids are not required by
      # Mongo, but they make certain operations more efficient.
      #
      # The driver does not automatically assign ids to records that are
      # inserted. (An upcoming feature will allow you to give an id "factory"
      # to a database and/or a collection.)
      # 
      #   12 bytes
      #   ---
      #    0 time
      #    1
      #    2
      #    3
      #    4 machine
      #    5
      #    6
      #    7 pid
      #    8
      #    9 inc
      #   10
      #   11
      class ObjectID

        MACHINE = ( val = rand(0x1000000); [val & 0xff, (val >> 8) & 0xff, (val >> 16) & 0xff] )
        PID = ( val = rand(0x10000); [val & 0xff, (val >> 8) & 0xff]; )

        # The string representation of an OID is different than its internal
        # and BSON byte representations. The BYTE_ORDER here maps
        # internal/BSON byte position (the index in BYTE_ORDER) to the
        # position of the two hex characters representing that byte in the
        # string representation. For example, the 0th BSON byte corresponds to
        # the (0-based) 7th pair of hex chars in the string.
        BYTE_ORDER = [7, 6, 5, 4, 3, 2, 1, 0, 11, 10, 9, 8]

        LOCK = Object.new
        LOCK.extend Mutex_m

        @@index_time = Time.new.to_i
        @@index = 0

        # Given a string representation of an ObjectID, return a new ObjectID
        # with that value.
        def self.from_string(str)
          raise "illegal ObjectID format" unless legal?(str)
          data = []
          BYTE_ORDER.each_with_index { |string_position, data_index|
            data[data_index] = str[string_position * 2, 2].to_i(16)
          }
          self.new(data)
        end

        def self.legal?(str)
          len = BYTE_ORDER.length * 2
          str =~ /([0-9a-f]+)/i
          match = $1
          str && str.length == len && match == str
        end

        # +data+ is an array of bytes. If nil, a new id will be generated.
        # The time +t+ is only used for testing; leave it nil.
        def initialize(data=nil, t=nil)
          @data = data || generate_id(t)
        end

        def eql?(other)
          @data == other.instance_variable_get("@data")
        end
        alias_method :==, :eql?

        def to_a
          @data.dup
        end

        def to_s
          str = ' ' * 24
          BYTE_ORDER.each_with_index { |string_position, data_index|
            str[string_position * 2, 2] = '%02x' % @data[data_index]
          }
          str
        end

        # (Would normally be private, but isn't so we can test it.)
        def generate_id(t=nil)
          t ||= Time.new.to_i
          buf = ByteBuffer.new
          buf.put_int(t & 0xffffffff)
          buf.put_array(MACHINE)
          buf.put_array(PID)
          i = index_for_time(t)
          buf.put(i & 0xff)
          buf.put((i >> 8) & 0xff)
          buf.put((i >> 16) & 0xff)

          buf.rewind
          buf.to_a.dup
        end

        # (Would normally be private, but isn't so we can test it.)
        def index_for_time(t)
          LOCK.mu_synchronize {
            if t != @@index_time
              @@index = 0
              @@index_time = t
            end
            retval = @@index
            @@index += 1
            retval
          }
        end

      end
    end
  end
end
