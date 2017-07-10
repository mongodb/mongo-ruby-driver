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
  module Operation
    module Write
      module Bulk

        # This module contains common functionality for merging results from
        # write commands during a bulk operation. Used for server versions >= 2.6.
        #
        # @since 2.0.0
        module Mergable

          # Aggregate the write errors returned from this result.
          #
          # @example Aggregate the write errors.
          #   result.aggregate_write_errors(0)
          #
          # @param [ Integer ] count The number of documents already executed.
          #
          # @return [ Array ] The aggregate write errors.
          #
          # @since 2.0.0
          def aggregate_write_errors(count)
            @replies.reduce(nil) do |errors, reply|
              if write_errors = reply.documents.first[Error::WRITE_ERRORS]
                wes = write_errors.collect do |we|
                  we.merge!('index' => count + we['index'])
                end
                (errors || []) << wes if wes
              end
            end
          end

          # Aggregate the write concern errors returned from this result.
          #
          # @example Aggregate the write concern errors.
          #   result.aggregate_write_concern_errors(100)
          #
          # @param [ Integer ] count The number of documents already executed.
          #
          # @return [ Array ] The aggregate write concern errors.
          #
          # @since 2.0.0
          def aggregate_write_concern_errors(count)
            @replies.each_with_index.reduce(nil) do |errors, (reply, _)|
              if write_concern_errors = reply.documents.first[Error::WRITE_CONCERN_ERRORS]
                (errors || []) << write_concern_errors.reduce(nil) do |errs, wce|
                    wce.merge!('index' => count + wce['index'])
                    (errs || []) << write_concern_error
                end
              end
            end
          end
        end
      end
    end
  end
end
