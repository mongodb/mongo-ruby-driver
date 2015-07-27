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

    # Raised if GridFS tries to do a read but there is no file information document
    #   found in the files collection.
    #
    # @since 2.1.0
    class NoFileInfo < Error

      # Create the new exception.
      #
      # @example Create the new exception.
      #   Mongo::Error::NoFileInfo.new
      #
      # @since 2.1.0
      def initialize
        super("No files information document found for the file requested. " +
                "The file either never existed, is in the process of being deleted, or has been corrupted.")
      end
    end
  end
end
