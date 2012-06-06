# encoding: UTF-8

# --
# Copyright (C) 2008-2011 10gen Inc.
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

module BSON

  def BSON::ObjectId(s)
    ObjectId.from_string(s)
  end

  # Generates MongoDB object ids.
  #
  # @core objectids
  class ObjectId
    attr_accessor :data

    # Create a new object id. If no parameter is given, an id corresponding
    # to the ObjectId BSON data type will be created. This is a 12-byte value
    # consisting of a 4-byte timestamp, a 3-byte machine id, a 2-byte process id,
    # and a 3-byte counter.
    #
    # @param [Array] data should be an array of bytes. If you want
    #   to generate a standard MongoDB object id, leave this argument blank.
    #
    # @option opts :data (nil) An array of bytes to use as the object id.
    # @option opts :time (nil) The value of this object ids timestamp. Note that
    #   the remaining bytes will consist of the standard machine id, pid, and counter. If
    #   you need a zeroed timestamp, used ObjectId.from_time.
    def initialize(data=nil, time=nil)
      @data = data || generate(time)
    end

    # Determine if the supplied string is legal. Legal strings will
    # consist of 24 hexadecimal characters.
    #
    # @param [String] str
    #
    # @return [Boolean]
    def self.legal?(str)
      str =~ /^[0-9a-f]{24}$/i ? true : false
    end

    # Create an object id from the given time. This is useful for doing range
    # queries; it works because MongoDB's object ids begin
    # with a timestamp.
    #
    # @param [Time] time a utc time to encode as an object id.
    #
    # @option opts [:unique] (false) If false, the object id's bytes
    #   succeeding the timestamp will be zeroed; if true, they'll
    #   consist of the standard machine id, pid, and counter.
    #
    # @return [BSON::ObjectId]
    #
    # @example Return all document created before Jan 1, 2010.
    #   time = Time.utc(2010, 1, 1)
    #   time_id = ObjectId.from_time(time)
    #   collection.find({'_id' => {'$lt' => time_id}})
    def self.from_time(time, opts={})
      unique = opts.fetch(:unique, false)
      if unique
        self.new(nil, time)
      else
        self.new([time.to_i,0,0].pack("NNN").unpack("C12"))
      end
    end

    # Adds a primary key to the given document if needed.
    #
    # @param [Hash] doc a document requiring an _id.
    #
    # @return [BSON::ObjectId, Object] returns a newly-created or 
    #   current _id for the given document.
    def self.create_pk(doc)
      doc.has_key?(:_id) || doc.has_key?('_id') ? doc : doc.merge!(:_id => self.new)
    end

    # Check equality of this object id with another.
    #
    # @param [BSON::ObjectId] object_id
    def eql?(object_id)
      object_id.kind_of?(BSON::ObjectId) and self.data == object_id.data
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

    # Given a string representation of an ObjectId, return a new ObjectId
    # with that value.
    #
    # @param [String] str
    #
    # @return [BSON::ObjectId]
    def self.from_string(str)
      raise InvalidObjectId, "illegal ObjectId format: #{str}" unless legal?(str)
      data = []
      12.times do |i|
        data[i] = str[i * 2, 2].to_i(16)
      end
      self.new(data)
    end

    # Get a string representation of this object id.
    #
    # @return [String]
    def to_s
      @data.map {|e| v=e.to_s(16); v.size == 1 ? "0#{v}" : v }.join
    end

    def inspect
      "BSON::ObjectId('#{to_s}')"
    end

    # Convert to MongoDB extended JSON format. Since JSON includes type information,
    # but lacks an ObjectId type, this JSON format encodes the type using an $oid key.
    #
    # @return [String] the object id represented as MongoDB extended JSON.
    def to_json(*a)
      "{\"$oid\": \"#{to_s}\"}"
    end

    # Create the JSON hash structure convert to MongoDB extended format. Rails 2.3.3 
    # introduced as_json to create the needed hash structure to encode objects into JSON.
    #
    # @return [Hash] the hash representation as MongoDB extended JSON
    def as_json(options ={})
      {"$oid" => to_s}
    end

    # Return the UTC time at which this ObjectId was generated. This may
    # be used in lieu of a created_at timestamp since this information
    # is always encoded in the object id.
    #
    # @return [Time] the time at which this object was created.
    def generation_time
      Time.at(@data.pack("C4").unpack("N")[0]).utc
    end

    def self.machine_id
      @@machine_id
    end

    private

    if RUBY_PLATFORM =~ /java/
      @@generator = Java::OrgBsonTypes::ObjectId
      @@machine_id = [@@generator.genMachineId].pack("N")[0,3]

      def generate(oid_time=nil)
        data = (oid_time ? @@generator.new(oid_time) : @@generator.new)

        oid = ''
        oid += [data.timeSecond].pack("N")
        oid += [data._machine].pack("N")
        oid += [data._inc].pack("N")
        oid.unpack("C12")
      end

    else
      @@lock  = Mutex.new
      @@index = 0
      @@machine_id = Digest::MD5.digest(Socket.gethostname)[0, 3]

      # We need to check whether BSON_CODER is defined because it's required by
      # the BSON C extensions.
      if defined?(BSON::BSON_CODER) && BSON::BSON_CODER == BSON::BSON_RUBY
        # This gets overwritten by the C extension if it loads.
        def generate(oid_time=nil)
          oid = ''

          # 4 bytes current time
          if oid_time
            t = oid_time.to_i
          else
            t = Time.new.to_i
          end
          oid += [t].pack("N")

          # 3 bytes machine
          oid += @@machine_id

          # 2 bytes pid
          oid += [Process.pid % 0xFFFF].pack("n")

          # 3 bytes inc
          oid += [get_inc].pack("N")[1, 3]

          oid.unpack("C12")
        end

        def get_inc
          @@lock.synchronize do
            @@index = (@@index + 1) % 0xFFFFFF
          end
        end
      end
    end
  end
end
