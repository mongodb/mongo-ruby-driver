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

    # Raised if a collation is specified for an operation but the server selected does not
    # support collations.
    #
    # @since 2.4.0
    class UnsupportedCollation < Error

      # Create the new exception.
      #
      # @example Create the new exception.
      #   Mongo::Error::UnsupportedCollation.new
      #
      # @since 2.4.0
      def initialize
        super("Collations is not a supported feature of the server handling this operation. " +
                "Operation results may be unexpected.")
      end
    end
  end
end
