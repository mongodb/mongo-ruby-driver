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

    # Raised if the URI is in the correct format but an option is provided that
    # is not recognized.
    #
    # @since 2.0.0
    class InvalidURIOption < Error

      # Create the error.
      #
      # @example Create the error with the invalid option name.
      #   InvalidURIOption.new('nothing')
      #
      # @param [ String ] name The invalid option name.
      #
      # @since 2.0.0
      def initialize(name)
        super("Invalid option in URI: '#{name}'.\n" +
          "Please see the following URL for more information: #{Mongo::URI::HELP}\n")
      end
    end
  end
end
