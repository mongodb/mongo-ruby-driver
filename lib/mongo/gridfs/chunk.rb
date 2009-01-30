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

        def initialize(file, mongo_object={})
          @file = file
          @object_id = mongo_object['_id'] || XGen::Mongo::Driver::ObjectID.new
          @chunk_number = mongo_object['n'] || 0

          @data = ByteBuffer.new
          case mongo_object['data']
          when String
            mongo_object['data'].each_byte { |b| @data.put(b) }
          when ByteBuffer
            @data.put_array(mongo_object['data'].to_a)
          when Array
            @data.put_array(mongo_object['data'])
          when nil
          else
            raise "illegal chunk format; data is #{mongo_object['data'] ? (' ' + mongo_object['data'].class.name) : 'nil'}"
          end
          @data.rewind
        end

        def pos; @data.position; end
        def pos=(pos); @data.position = pos; end
        def eof?; !@data.more?; end

        def size; @data.size; end
        alias_method :length, :size

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
          coll = @file.chunk_collection
          coll.remove({'_id' => @object_id}) if @object_id
          coll.insert(to_mongo_object)
        end

        def to_mongo_object
          h = OrderedHash.new
          h['_id'] = @object_id
          h['files_id'] = @file.files_id
          h['n'] = @chunk_number
          h['data'] = data
          h
        end

      end
    end
  end
end
