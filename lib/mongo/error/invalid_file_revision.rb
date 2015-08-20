# Copyright (C) 2014-2015 MongoDB, Inc.
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

    # Raised if the requested file revision is not found.
    #
    # @since 2.1.0
    class InvalidFileRevision < Error

      # Create the new exception.
      #
      # @example Create the new exception.
      #   Mongo::Error::InvalidFileRevision.new('some-file.txt', 3)
      #
      # @param [ String ] filename The name of the file.
      # @param [ Integer ] revision The requested revision.
      #
      # @since 2.1.0
      def initialize(filename, revision)
        super("No revision #{revision} found for file '#{filename}'.")
      end
    end
  end
end
