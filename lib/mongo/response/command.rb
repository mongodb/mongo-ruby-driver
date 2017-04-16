# Copyright (C) 2009-2014 MongoDB, Inc.
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
  module Response

    # A response object for Commands to the database.
    #
    # @since 2.0.0
    class Command
      include Responsive

      # A catch-all for any data returned by a Command.
      #
      # @return [ Hash ] command data.
      #
      # @since 2.0.0
      def data
        {}
      end

      # Meta-programming to allow us to gracefully handle any type of command.
      # If the requested method is a field within this command's data, return the
      # value of that field.  Otherwise, raise a NoMethodError.
      #
      # @return [ Hash ] result... this doesn't have to be a hash.
      #
      # @since 2.0.0
      def method_missing(m)
        return data[m] if data[m]
        raise NoMethodError, 'Command does not have response field #{m}'
      end
    end
  end
end
