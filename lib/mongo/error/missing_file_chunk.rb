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
  class Error

    # Raised if the next chunk when reading from a GridFSBucket does not have the
    # expected sequence number (n).
    #
    # @since 2.1.0
    class MissingFileChunk < Error

      # Create the new exception.
      #
      # @example Create the new exception.
      #   Mongo::Error::MissingFileChunk.new(expected_n, chunk)
      #
      # @param [ Integer ] expected_n The expected index value.
      # @param [ Grid::File::Chunk | Integer ] chunk The chunk read from GridFS.
      #
      # @since 2.1.0
      #
      # @api private
      def initialize(expected_n, chunk)
        if chunk.is_a?(Integer)
          super("Missing chunk(s). Expected #{expected_n} chunks but got #{chunk}.")
        else
          super("Unexpected chunk in sequence. Expected next chunk to have index #{expected_n} but it has index #{chunk.n}")
        end
      end
    end
  end
end
