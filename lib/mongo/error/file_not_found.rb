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

    # Raised if a file is deleted from a GridFS but it is not found.
    #
    # @since 2.1.0
    class FileNotFound < Error

      # Create the new exception.
      #
      # @example Create the new exception.
      #   Mongo::Error::FileNotFound.new(id, :id)
      #
      # @param [ Object ] value The property value used to find the file.
      # @param [ String, Symbol ] property The name of the property used to find the file.
      #
      # @since 2.1.0
      def initialize(value, property)
        super("File with #{property} '#{value}' not found.")
      end
    end
  end
end
