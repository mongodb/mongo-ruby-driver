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

require 'thread'
require 'socket'
require 'digest/md5'

module Mongo

  # ObjectID class for documents in MongoDB.
  class ObjectID
    # This is the legacy byte ordering for Babble. Versions of the Ruby
    # driver prior to 0.14 used this byte ordering when converting ObjectID
    # instances to and from strings. If you have string representations of
    # ObjectIDs using the legacy byte ordering make sure to use the
    # to_s_legacy and from_string_legacy methods, or convert your strings
    # with ObjectID#legacy_string_convert
    BYTE_ORDER = [7, 6, 5, 4, 3, 2, 1, 0, 11, 10, 9, 8]

    @@lock  = Mutex.new
    @@index = 0

    # Create a new object id. If no parameter is given, an id corresponding
    # to the ObjectID BSON data type will be created. This is a 12-byte value 
    # consisting of a 4-byte timestamp, a 3-byte machine id, a 2-byte process id,
    # and a 3-byte counter.
    #
    # @param [Array] data should be an array of bytes. If you want
    #   to generate a standard MongoDB object id, leave this argument blank.
    def initialize(data=nil)
      @data = data || generate
    end

    def self.legal?(str)
      len = BYTE_ORDER.length * 2
      str =~ /([0-9a-f]+)/i
      match = $1
      str && str.length == len && match == str
    end

    # Create an object id from the given time. This is useful for doing range
    # queries; it works because MongoDB's object ids begin
    # with a timestamp.
    #
    # @param [Time] time a utc time to encode as an object id.
    #
    # @return [Mongo::ObjectID]
    #
    # @example Return all document created before Jan 1, 2010.
    #   time = Time.utc(2010, 1, 1)
    #   time_id = ObjectID.from_time(time)
    #   collection.find({'_id' => {'$lt' => time_id}})
    def self.from_time(time)
      self.new([time.to_i,0,0].pack("NNN").unpack("C12"))
    end

    # Adds a primary key to the given document if needed.
    #
    # @param [Hash] doc a document requiring an _id.
    #
    # @return [Mongo::ObjectID, Object] returns a newly-created or 
    #   current _id for the given document.
    def self.create_pk(doc)
      doc.has_key?(:_id) || doc.has_key?('_id') ? doc : doc.merge!(:_id => self.new)
    end

    # Check equality of this object id with another.
    #
    # @param [Mongo::ObjectID] object_id
    def eql?(object_id)
      @data == object_id.instance_variable_get("@data")
    end
    alias_method :==, :eql?

    # Get a unique hashcode for this object.
    # This is required since we've defined an #eql? method.
    #
    # @return [Integer]
    def hash
      @data.hash
    end

    # Get an array representation of the object id.
    #
    # @return [Array]
    def to_a
      @data.dup
    end

    # Given a string representation of an ObjectID, return a new ObjectID
    # with that value.
    #
    # @param [String] str
    #
    # @return [Mongo::ObjectID]
    def self.from_string(str)
      raise InvalidObjectID, "illegal ObjectID format" unless legal?(str)
      data = []
      12.times do |i|
        data[i] = str[i * 2, 2].to_i(16)
      end
      self.new(data)
    end

    # @deprecated
    # Create a new ObjectID given a string representation of an ObjectID
    # using the legacy byte ordering. This method may eventually be
    # removed. If you are not sure that you need this method you should be
    # using the regular from_string.
    def self.from_string_legacy(str)
      warn "Support for legacy object ids has been DEPRECATED."
      raise InvalidObjectID, "illegal ObjectID format" unless legal?(str)
      data = []
      BYTE_ORDER.each_with_index { |string_position, data_index|
        data[data_index] = str[string_position * 2, 2].to_i(16)
      }
      self.new(data)
    end

    # Get a string representation of this object id.
    #
    # @return [String]
    def to_s
      str = ' ' * 24
      12.times do |i|
        str[i * 2, 2] = '%02x' % @data[i]
      end
      str
    end
    alias_method :inspect, :to_s

    # Convert to MongoDB extended JSON format. Since JSON includes type information,
    # but lacks an ObjectID type, this JSON format encodes the type using an $id key.
    #
    # @return [String] the object id represented as MongoDB extended JSON.
    def to_json(escaped=false)
      "{\"$oid\": \"#{to_s}\"}"
    end

    # @deprecated
    # Get a string representation of this ObjectID using the legacy byte
    # ordering. This method may eventually be removed. If you are not sure
    # that you need this method you should be using the regular to_s.
    def to_s_legacy
      warn "Support for legacy object ids has been DEPRECATED."
      str = ' ' * 24
      BYTE_ORDER.each_with_index { |string_position, data_index|
        str[string_position * 2, 2] = '%02x' % @data[data_index]
      }
      str
    end

    # @deprecated
    # Convert a string representation of an ObjectID using the legacy byte
    # ordering to the proper byte ordering. This method may eventually be
    # removed. If you are not sure that you need this method it is probably
    # unnecessary.
    def self.legacy_string_convert(str)
      warn "Support for legacy object ids has been DEPRECATED."
      legacy = ' ' * 24
      BYTE_ORDER.each_with_index do |legacy_pos, pos|
        legacy[legacy_pos * 2, 2] = str[pos * 2, 2]
      end
      legacy
    end

    # Return the UTC time at which this ObjectID was generated. This may
    # be used in lieu of a created_at timestamp since this information
    # is always encoded in the object id.
    #
    # @return [Time] the time at which this object was created.
    def generation_time
      Time.at(@data.pack("C4").unpack("N")[0]).utc
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
      @@lock.synchronize do
        @@index = (@@index + 1) % 0xFFFFFF
      end
    end
  end
end
