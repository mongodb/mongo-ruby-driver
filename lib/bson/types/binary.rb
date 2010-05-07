# encoding: UTF-8

# --
# Copyright (C) 2008-2010 10gen Inc.
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

require 'bson/byte_buffer'

module BSON

  # An array of binary bytes with a MongoDB subtype. See the subtype
  # constants for reference.
  #
  # Use this class when storing binary data in documents.
  class Binary < ByteBuffer

    SUBTYPE_BYTES = 0x02
    SUBTYPE_UUID = 0x03
    SUBTYPE_MD5 = 0x05
    SUBTYPE_USER_DEFINED = 0x80

    # One of the SUBTYPE_* constants. Default is SUBTYPE_BYTES.
    attr_accessor :subtype

    # Create a buffer for storing binary data in MongoDB.
    #
    # @param [Array, String] data to story as BSON binary. If a string is given, the value will be
    #   concerted to an array of bytes using String#unpack("c*").
    # @param [Fixnum] one of four values specifying a BSON binary subtype. Possible values are
    #   SUBTYPE_BYTES, SUBTYPE_UUID, SUBTYPE_MD5, and SUBTYPE_USER_DEFINED.
    #
    # @see http://www.mongodb.org/display/DOCS/BSON#BSON-noteondatabinary BSON binary subtypes.
    def initialize(data=[], subtype=SUBTYPE_BYTES)
      data = data.unpack("c*") if data.is_a?(String)
      super(data)
      @subtype = subtype
    end

    def inspect
      "<BSON::Binary:#{object_id}>"
    end

  end
end
