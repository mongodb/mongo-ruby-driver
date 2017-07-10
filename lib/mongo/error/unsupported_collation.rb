# Copyright (C) 2014-2017 MongoDB, Inc.
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

    # Raised if a collation is specified for an operation but the server selected does not
    # support collations.
    #
    # @since 2.4.0
    class UnsupportedCollation < Error

      # The default error message describing that collations is not supported.
      #
      # @return [ String ] A default message describing that collations is not supported by the server.
      #
      # @since 2.4.0
      DEFAULT_MESSAGE = "Collations is not a supported feature of the server handling this operation. " +
          "Operation results may be unexpected."

      # The error message describing that collations cannot be used when write concern is unacknowledged.
      #
      # @return [ String ] A message describing that collations cannot be used when write concern is unacknowledged.
      #
      # @since 2.4.0
      UNACKNOWLEDGED_WRITES_MESSAGE = "A collation cannot be specified when using unacknowledged writes. " +
        "Either remove the collation option or use acknowledged writes (w >= 1)."

      # Create the new exception.
      #
      # @example Create the new exception.
      #   Mongo::Error::UnsupportedCollation.new
      #
      # @since 2.4.0
      def initialize(message = nil)
        super(message || DEFAULT_MESSAGE)
      end
    end
  end
end
