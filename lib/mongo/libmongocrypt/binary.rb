# Copyright (C) 2019 MongoDB, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

require 'ffi'
require 'byebug' # TODO: remove

module Mongo
  module Libmongocrypt

    # TODO: class description
    #
    # @since 2.12.0
    class Binary
      def initialize(data_string)
        unless data_string
          raise MongocryptError.new('Cannot create new Binary object with no data')
        end

        # TODO: error handling
        # TODO: make sure keeping copy of data
        @data = data_string.unpack('C*')

        data_p = FFI::MemoryPointer.new(@data.length)
        data_p.write_array_of_type(FFI::TYPE_UINT8, :put_uint8, @data)

        @bin = Binding.mongocrypt_binary_new_from_data(data_p, @data.length)
      end

      def to_bytes
        data = Binding.mongocrypt_binary_data(@bin)
        if data == FFI::Pointer::NULL
          return []
        end

        len = Binding.mongocrypt_binary_len(@bin)
        data.read_array_of_type(FFI::TYPE_UINT8, :read_uint8, len)
      end

      def close
        # TODO: error handling
        # TODO: make sure this also frees everything??
        Binding.mongocrypt_binary_destroy(@bin) if @bin
        @data = nil if @data
      end
    end
  end
end
