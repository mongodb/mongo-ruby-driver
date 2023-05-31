# frozen_string_literal: true

# Copyright (C) 2016-2023 MongoDB Inc.
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
  class Server
    class AppMetadata
      # Implements the metadata truncation logic described in the handshake
      # spec.
      #
      # @api private
      class Truncator
        # @return [ BSON::Document ] the document being truncated.
        attr_reader :document

        # The max application metadata document byte size.
        MAX_DOCUMENT_SIZE = 512

        # Creates a new Truncator instance and tries enforcing the maximum
        # document size on the given document.
        #
        # @param [ BSON::Document] document The document to (potentially)
        #   truncate.
        #
        # @note The document is modified in-place; if you wish to keep the
        #   original unchanged, you must deep-clone it prior to sending it to
        #   a truncator.
        def initialize(document)
          @document = document
          try_truncate!
        end

        # The current size of the document, in bytes, as a serialized BSON
        # document.
        #
        # @return [ Integer ] the size of the document
        def size
          @document.to_bson.to_s.length
        end

        # Whether the document fits within the required maximum document size.
        #
        # @return [ true | false ] if the document is okay or not.
        def ok?
          size <= MAX_DOCUMENT_SIZE
        end

        private

        # How many extra bytes must be trimmed before the document may be
        # considered #ok?.
        #
        # @return [ Integer ] how many bytes larger the document is than the
        #   maximum document size.
        def excess
          size - MAX_DOCUMENT_SIZE
        end

        # Attempt to truncate the document using the documented metadata
        # priorities (see the handshake specification).
        def try_truncate!
          %i[ env_fields os_fields env platform ].each do |target|
            break if ok?

            send(:"try_truncate_#{target}!")
          end
        end

        # Attempt to truncate or remove the {{:platform}} key from the
        # document.
        def try_truncate_platform!
          @document.delete(:platform) unless try_truncate_string(@document[:platform])
        end

        # Attempt to truncate the keys in the {{:env}} subdocument.
        def try_truncate_env_fields!
          try_truncate_hash(@document[:env], reserved: %w[ name ])
        end

        # Attempt to truncate the keys in the {{:os}} subdocument.
        def try_truncate_os_fields!
          try_truncate_hash(@document[:os], reserved: %w[ type ])
        end

        # Remove the {{:env}} key from the document.
        def try_truncate_env!
          @document.delete(:env)
        end

        # A helper method for truncating a string (in-place) by whatever
        # {{#excess}} is required.
        #
        # @param [ String ] string the string value to truncate.
        #
        # @note the parameter is modified in-place.
        def try_truncate_string(string)
          length = string&.length || 0

          return false if excess > length

          string[(length - excess)..-1] = ''
        end

        # A helper method for removing the keys of a Hash (in-place) until
        # the document is the necessary size. The keys are considered in order
        # (using the Hash's native key ordering), and each will be removed from
        # the hash in turn, until the document is the necessary size.
        #
        # Any keys in the {{reserved}} list will be ignored.
        #
        # @param [ Hash | nil ] hash the Hash instance to consider.
        # @param [ Array ] reserved the list of keys to ignore in the hash.
        #
        # @note the hash parameter is modified in-place.
        def try_truncate_hash(hash, reserved: [])
          return false unless hash

          keys = hash.keys - reserved
          keys.each do |key|
            hash.delete(key)

            return true if ok?
          end

          false
        end
      end
    end
  end
end
