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

require 'mutex_m'
require 'socket'
require 'digest/md5'
require 'mongo/util/byte_buffer'

module Mongo

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
    # The string representation of an OID is different than its internal
    # and BSON byte representations. The BYTE_ORDER here maps
    # internal/BSON byte position (the index in BYTE_ORDER) to the
    # position of the two hex characters representing that byte in the
    # string representation. For example, the 0th BSON byte corresponds to
    # the (0-based) 7th pair of hex chars in the string.
    BYTE_ORDER = [7, 6, 5, 4, 3, 2, 1, 0, 11, 10, 9, 8]

    LOCK = Object.new
    LOCK.extend Mutex_m

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
    def initialize(data=nil)
      @data = data || generate
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

    private

    def generate
      # 4 bytes current time
      time = Time.new.to_i
      buf = ByteBuffer.new
      buf.put_int(time & 0xFFFFFFFF)

      # 3 bytes machine
      machine_hash = Digest::MD5.digest(Socket.gethostname)
      buf.put(machine_hash[0])
      buf.put(machine_hash[1])
      buf.put(machine_hash[2])

      # 2 bytes pid
      pid = Process.pid % 0xFFFF
      buf.put(pid & 0xFF)
      buf.put((pid >> 8) & 0xFF)

      # 3 bytes inc
      inc = get_inc
      buf.put(inc & 0xFF)
      buf.put((inc >> 8) & 0xFF)
      buf.put((inc >> 16) & 0xFF)

      buf.rewind
      buf.to_a.dup
    end

    def get_inc
      LOCK.mu_synchronize {
        @@index = (@@index + 1) % 0xFFFFFF
      }
    end
  end
end
