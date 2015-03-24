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
  module Operation
    module Write

      module BulkMergable

        # Aggregate the write errors returned from this result.
        #
        # @example Aggregate the write errors.
        #   result.aggregate_write_errors([0, 1, 2, 3])
        #
        # @param [ Array ] indexes The indexes of each operation as they
        #   were listed in the Bulk API.
        #
        # @return [ Array ] The aggregate write errors.
        #
        # @since 2.0.0
        def aggregate_write_errors(indexes)
          @replies.reduce(nil) do |errors, reply|
            if reply.documents.first['writeErrors']
              write_errors = reply.documents.first['writeErrors'].collect do |we|
                we.merge!('index' => indexes[we['index']])
              end
              (errors || []) << write_errors if write_errors
            end
          end
        end

        # Aggregate the write concern errors returned from this result.
        #
        # @example Aggregate the write concern errors.
        #   result.aggregate_write_concern_errors([0, 1, 2, 3])
        #
        # @param [ Array ] indexes The indexes of each operation as they
        #   were listed in the Bulk API.
        #
        # @return [ Array ] The aggregate write concern errors.
        #
        # @since 2.0.0
        def aggregate_write_concern_errors(indexes)
          @replies.each_with_index.reduce(nil) do |errors, (reply, i)|
            if write_concern_errors = reply.documents.first['writeConcernErrors']
              (errors || []) << write_concern_errors.reduce(nil) do |errs, wce|
                  wce.merge!('index' => indexes[wce['index']])
                  (errs || []) << write_concern_error
              end
            end
          end
        end
      end
    end
  end
end
