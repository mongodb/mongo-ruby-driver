require 'mongo/util/byte_buffer'

module XGen
  module Mongo
    module Driver

      class MessageHeader

        HEADER_SIZE = 16

        def initialize()
          @buf = ByteBuffer.new
        end

        def read_header(socket)
          @buf.rewind
          @buf.put_array(socket.recv(HEADER_SIZE).unpack("C*"))
          raise "Short read for DB response header: expected #{HEADER_SIZE} bytes, saw #{@buf.size}" unless @buf.size == HEADER_SIZE
          @buf.rewind
          @size = @buf.get_int
          @request_id = @buf.get_int
          @response_to = @buf.get_int
          @op = @buf.get_int
          self
        end

        def dump
          @buf.dump
        end
      end
    end
  end
end

