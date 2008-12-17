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

        LOCK = Object.new
        LOCK.extend Mutex_m

        @@index_time = Time.new.to_i
        @@index = 0

        # +data+ is an array of bytes. If nil, a new id will be generated.
        # The time +t+ is only used for testing; leave it nil.
        def initialize(data=nil, t=nil)
          @data = data || generate_id(t)
        end

        def eql?(other)
          @data == other.to_a
        end
        alias_method :==, :eql?

        def to_a
          @data.dup
        end

        def to_s
          @data.collect { |b| '%02x' % b }.join
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
