# frozen_string_literal: true
# encoding: utf-8

# Copyright (C) 2014-2022 MongoDB Inc.
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

    # A Hash that caches the results of #to_bson.
    #
    # @api private
    class CachingHash

      def initialize(hash)
        @hash = hash
      end

      def bson_type
        Hash::BSON_TYPE
      end

      # Caches the result of to_bson and writes it to the given buffer on subsequent
      # calls to this method. If this method is originally called without validation,
      # and then is subsequently called with validation, we will want to recalculate
      # the to_bson to trigger the validations.
      #
      # @param [ BSON::ByteBuffer ] buffer The encoded BSON buffer to append to.
      # @param [ true, false ] validating_keys Whether keys should be validated when serializing.
      #
      # @return [ BSON::ByteBuffer ] The buffer with the encoded object.
      def to_bson(buffer = BSON::ByteBuffer.new, validating_keys = BSON::Config.validating_keys?)
        if !@bytes
          @bytes = @hash.to_bson(BSON::ByteBuffer.new, validating_keys).to_s
        elsif needs_validation?(validating_keys)
          @validated = true
          return @hash.to_bson(buffer, validating_keys)
        end
        @validated ||= validating_keys
        buffer.put_bytes(@bytes)
      end

      private

      # Checks the current value for validating keys, and whether or not this
      # bson has been validated in the past, and decides if we need to recalculate
      # the to_bson to check the validations.
      #
      # @param [ true, false ] validating_keys Whether keys should be validated when serializing.
      #
      # @return [ true, false ] Whether or not the bson needs to be recalculated
      #   with validation.
      def needs_validation?(validating_keys)
        !@validated && validating_keys
      end
    end
  end
end
