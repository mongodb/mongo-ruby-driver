# frozen_string_literal: true
# rubocop:todo all

# Copyright (C) 2014-2020 MongoDB Inc.
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

module Mongo
  module Protocol
    module Serializers
      # Class used to define a bitvector for a MongoDB wire protocol message.
      #
      # Defines serialization strategy upon initialization.
      #
      # @api private
      class BitVector

        # Initializes a BitVector with a layout
        #
        # @param layout [ Array<Symbol> ] the array of fields in the bit vector
        def initialize(layout)
          @masks = {}
          layout.each_with_index do |field, index|
            @masks[field] = 2**index if field
          end
        end

        # Serializes vector by encoding each symbol according to its mask
        #
        # @param buffer [ String ] Buffer to receive the serialized vector
        # @param value [ Array<Symbol> ] Array of flags to encode
        # @param [ true, false ] validating_keys Whether keys should be validated when serializing.
        #   This option is deprecated and will not be used. It will removed in version 3.0.
        #
        # @return [ String ] Buffer that received the serialized vector
        def serialize(buffer, value, validating_keys = nil)
          bits = 0
          value.each { |flag| bits |= (@masks[flag] || 0) }
          buffer.put_int32(bits)
        end

        # Deserializes vector by decoding the symbol according to its mask
        #
        # @param [ String ] buffer Buffer containing the vector to be deserialized.
        # @param [ Hash ] options This method does not currently accept any options.
        #
        # @return [ Array<Symbol> ] Flags contained in the vector
        def deserialize(buffer, options = {})
          vector = buffer.get_int32
          flags = []
          @masks.each do |flag, mask|
            flags << flag if mask & vector != 0
          end
          flags
        end
      end
    end
  end
end
