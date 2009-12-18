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

module Mongo

  # Representation of an ObjectId for Mongo.
  class ObjectID
    # This is the legacy byte ordering for Babble. Versions of the Ruby
    # driver prior to 0.14 used this byte ordering when converting ObjectID
    # instances to and from strings. If you have string representations of
    # ObjectIDs using the legacy byte ordering make sure to use the
    # to_s_legacy and from_string_legacy methods, or convert your strings
    # with ObjectID#legacy_string_convert
    BYTE_ORDER = [7, 6, 5, 4, 3, 2, 1, 0, 11, 10, 9, 8]

    LOCK = Object.new
    LOCK.extend Mutex_m

    @@index = 0

    def self.legal?(str)
      len = BYTE_ORDER.length * 2
      str =~ /([0-9a-f]+)/i
      match = $1
      str && str.length == len && match == str
    end

    # Adds a primary key to the given document if needed.
    def self.create_pk(doc)
      doc.has_key?(:_id) || doc.has_key?('_id') ? doc : doc.merge!(:_id => self.new)
    end

    # +data+ is an array of bytes. If nil, a new id will be generated.
    def initialize(data=nil)
      @data = data || generate
    end

    def eql?(other)
      @data == other.instance_variable_get("@data")
    end
    alias_method :==, :eql?

    # Returns a unique hashcode for the object.
    # This is required since we've defined an #eql? method.
    def hash
      @data.hash
    end

    def to_a
      @data.dup
    end

    # Given a string representation of an ObjectID, return a new ObjectID
    # with that value.
    def self.from_string(str)
      raise InvalidObjectID, "illegal ObjectID format" unless legal?(str)
      data = []
      12.times do |i|
        data[i] = str[i * 2, 2].to_i(16)
      end
      self.new(data)
    end

    # Create a new ObjectID given a string representation of an ObjectID
    # using the legacy byte ordering. This method may eventually be
    # removed. If you are not sure that you need this method you should be
    # using the regular from_string.
    def self.from_string_legacy(str)
      raise InvalidObjectID, "illegal ObjectID format" unless legal?(str)
      data = []
      BYTE_ORDER.each_with_index { |string_position, data_index|
        data[data_index] = str[string_position * 2, 2].to_i(16)
      }
      self.new(data)
    end

    def to_s
      str = ' ' * 24
      12.times do |i|
        str[i * 2, 2] = '%02x' % @data[i]
      end
      str
    end

    def inspect; to_s; end

    # Get a string representation of this ObjectID using the legacy byte
    # ordering. This method may eventually be removed. If you are not sure
    # that you need this method you should be using the regular to_s.
    def to_s_legacy
      str = ' ' * 24
      BYTE_ORDER.each_with_index { |string_position, data_index|
        str[string_position * 2, 2] = '%02x' % @data[data_index]
      }
      str
    end

    # Convert a string representation of an ObjectID using the legacy byte
    # ordering to the proper byte ordering. This method may eventually be
    # removed. If you are not sure that you need this method it is probably
    # unnecessary.
    def self.legacy_string_convert(str)
      legacy = ' ' * 24
      BYTE_ORDER.each_with_index do |legacy_pos, pos|
        legacy[legacy_pos * 2, 2] = str[pos * 2, 2]
      end
      legacy
    end

    # Returns the utc time at which this ObjectID was generated. This may
    # be used in lieu of a created_at timestamp.
    def generation_time
      Time.at(@data.pack("C4").unpack("N")[0])
    end

    private

    # We need to define this method only if CBson isn't loaded.
    unless defined? CBson
      def generate
        oid = ''

        # 4 bytes current time
        time = Time.new.to_i
        oid += [time].pack("N")

        # 3 bytes machine
        oid += Digest::MD5.digest(Socket.gethostname)[0, 3]

        # 2 bytes pid
        oid += [Process.pid % 0xFFFF].pack("n")

        # 3 bytes inc
        oid += [get_inc].pack("N")[1, 3]

        oid.unpack("C12")
      end
    end

    def get_inc
      LOCK.mu_synchronize {
        @@index = (@@index + 1) % 0xFFFFFF
      }
    end
  end
end
