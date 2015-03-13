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

require 'mongo/bulk_write/bulk_writable'
require 'mongo/bulk_write/ordered_bulk_write'
require 'mongo/bulk_write/unordered_bulk_write'

module Mongo
  module BulkWrite
    extend self

    # Get a bulk write object either of type ordered or unordered.
    #
    # @example Get a bulk write object.
    #   Mongo::BulkWrite.get(collection, operations, ordered: true)
    #
    # @param [ Collection ] collection The collection on which the operations
    #   will be executed.
    #
    # @param [ Array<Hash> ] operations The operations to execute.
    #
    # @param [ Hash ] options The options for the bulk write object.
    #
    # @option options [ true, false ] :ordered Whether the operations
    #   should be executed in order.
    # @option options [ Hash ] :write_concern The write concern options.
    #   Can be :w => Integer, :fsync => Boolean, :j => Boolean.
    #
    # @return [ OrderedBulkWrite, UnorderedBulkWrite ] The appropriate bulk
    #   write object.
    #
    # @since 2.0.0
    def get(collection, operations, options)
      if options.fetch(:ordered, true)
        OrderedBulkWrite.new(collection, operations, options)
      else
        UnorderedBulkWrite.new(collection, operations, options)
      end
    end
  end
end
