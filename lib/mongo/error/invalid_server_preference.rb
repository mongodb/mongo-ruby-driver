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

    # Raised when an invalid server preference is provided.
    #
    # @since 2.0.0
    class InvalidServerPreference < Error

      # Instantiate the new exception.
      #
      # @example Instantiate the exception.
      #   Mongo::ServerSelector::InvalidServerPreference.new
      #
      # @param [ String ] name The preference name.
      #
      # @since 2.0.0
      def initialize(name)
        super("This server preference #{name} cannot be combined with tags.")
      end
    end
  end
end
