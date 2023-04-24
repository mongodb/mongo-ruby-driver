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
    # expected length.
    #
    # @since 2.1.0
    class UnexpectedChunkLength < Error

      # Create the new exception.
      #
      # @example Create the new exception.
      #   Mongo::Error::UnexpectedChunkLength.new(expected_len, chunk)
      #
      # @param [ Integer ] expected_len The expected length.
      # @param [ Grid::File::Chunk ] chunk The chunk read from GridFS.
      #
      # @since 2.1.0
      def initialize(expected_len, chunk)
        super("Unexpected chunk length. Chunk has length #{chunk.data.data.size} but expected length " +
          "#{expected_len} or for it to be the last chunk in the sequence.")
      end
    end
  end
end
