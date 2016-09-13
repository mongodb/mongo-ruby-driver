# Copyright (C) 2016 MongoDB, Inc.
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

    # This exception is raised when the metadata document sent to the server
    #   at the time of a connection handshake is invalid.
    #
    # @since 2.4.0
    class InvalidApplicationName < Error

      # Instantiate the new exception.
      #
      # @example Create the exception.
      #   InvalidApplicationName.new(app_name, 128)
      #
      # @param [ String ] app_name The application name option.
      # @param [ Integer ] max_size The max byte size of the application name.
      #
      # @since 2.4.0
      def initialize(app_name, max_size)
        super("The provided application name '#{app_name}' cannot exceed #{max_size} bytes.")
      end
    end
  end
end
