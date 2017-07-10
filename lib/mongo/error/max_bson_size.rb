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

    # Exception that is raised when trying to serialize a document that
    # exceeds max BSON object size.
    #
    # @since 2.0.0
    class MaxBSONSize < Error

      # The message is constant.
      #
      # @since 2.0.0
      MESSAGE = "Document exceeds allowed max BSON size.".freeze

      # Instantiate the new exception.
      #
      # @example Instantiate the exception.
      #   Mongo::Error::MaxBSONSize.new(max)
      #
      # @since 2.0.0
      def initialize(max_size = nil)
        super(max_size ?  MESSAGE + " The max is #{max_size}." : MESSAGE)
      end
    end
  end
end
