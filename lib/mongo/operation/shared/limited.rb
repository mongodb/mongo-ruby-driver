# Copyright (C) 2015-2020 MongoDB Inc.
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
  module Operation

    # Shared behavior of operations that require its documents to each have an id.
    #
    # @since 2.5.2
    module Limited

      private

      # Get the options for executing the operation on a particular connection.
      #
      # @param [ Server::Connection ] connection The connection that the
      #   operation will be executed on.
      #
      # @return [ Hash ] The options.
      #
      # @since 2.0.0
      def options(connection)
        super.merge(limit: -1)
      end
    end
  end
end
