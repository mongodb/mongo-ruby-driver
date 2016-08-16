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
  module Operation
    module Commands

      # A MongoDB parallel scan operation.
      #
      # @example Create the parallel scan operation.
      #   ParallelScan.new({
      #     :db_name  => 'test_db',
      #     :coll_name = > 'test_collection',
      #     :cursor_count => 5
      #   })
      #
      # Initialization:
      #   param [ Hash ] spec The specifications for the operation.
      #
      #   option spec :db_name [ String ] The name of the database on which
      #     the operation should be executed.
      #   option spec :coll_name [ String ] The collection to scan.
      #   option spec :cursor_count [ Integer ] The number of cursors to use.
      #   option spec :options [ Hash ] Options for the command.
      #
      # @since 2.0.0
      class ParallelScan < Command

        private

        def selector
          command = { :parallelCollectionScan => coll_name, :numCursors => cursor_count }
          command[:readConcern] = read_concern if read_concern
          command[:maxTimeMS] = max_time_ms if max_time_ms
          command
        end
      end
    end
  end
end

require 'mongo/operation/commands/parallel_scan/result'
