require 'mongo/types/binary'
require 'mongo/types/dbref'
require 'mongo/types/objectid'
require 'mongo/util/byte_buffer'
require 'mongo/util/ordered_hash'


module XGen
  module Mongo
    module GridFS

      # A chunk stores a portion of GridStore data.
      #
      # TODO: user-defined chunk size
      class Chunk

        DEFAULT_CHUNK_SIZE = 1024 * 256

        attr_reader :object_id, :chunk_number
        attr_accessor :data

        def initialize(db_collection, mongo_object={})
          @coll = db_collection
          @object_id = mongo_object['_id'] || XGen::Mongo::Driver::ObjectID.new
          @chunk_number = mongo_object['cn'] || 1

          @data = ByteBuffer.new
          case mongo_object['data']
          when String
            mongo_object['data'].each_byte { |b| @data.put(b) }
          when ByteBuffer
            @data.put_array(mongo_object['data'].to_a)
          when Array
            @data.put_array(mongo_object['data'])
          end
          @data.rewind

          @next_chunk_dbref = mongo_object['next']
        end

        def has_next?
          @next_chunk_dbref
        end

        def next
          return @next_chunk if @next_chunk
          return nil unless @next_chunk_dbref
          row = @coll.find({'_id' => @next_chunk_dbref.object_id}).next_object
          @next_chunk = self.class.new(@coll, row) if row
          @next_chunk
        end

        def next=(chunk)
          @next_chunk = chunk
          @next_chunk_dbref = XGen::Mongo::Driver::DBRef.new(nil, nil, @coll.db, '_chunks', chunk.object_id)
        end

        def pos; @data.position; end
        def pos=(pos); @data.position = pos; end
        def eof?; !@data.more?; end

        def size; @data.size; end
        alias_method :length, :size

        def empty?
          @data.length == 0
        end

        def clear
          @data.clear
        end

        # Erase all data after current position.
        def truncate
          if @data.position < @data.length
            curr_data = @data
            @data = ByteBuffer.new
            @data.put_array(curr_data.to_a[0...curr_data.position])
          end
        end

        def getc
          @data.more? ? @data.get : nil
        end

        def putc(byte)
          @data.put(byte)
        end

        def save
          @coll.remove({'_id' => @object_id}) if @object_id
          @coll.insert(to_mongo_object)
        end

        def to_mongo_object
          h = OrderedHash.new
          h['_id'] = @object_id
          h['cn'] = @chunk_number
          h['data'] = data
          h['next'] = @next_chunk_dbref
          h
        end

      end
    end
  end
end
