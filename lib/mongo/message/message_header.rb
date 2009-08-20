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

require 'mongo/util/byte_buffer'

module Mongo

  class MessageHeader

    HEADER_SIZE = 16

    def initialize()
      @buf = ByteBuffer.new
    end

    def read_header(db)
      @buf.rewind
      @buf.put_array(db.receive_full(HEADER_SIZE).unpack("C*"))
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
