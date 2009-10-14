# --
# Copyright (C) 2008-2009 10gen Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
# ++

require 'mongo/collection'
require 'mongo/message'
require 'mongo/types/code'

module Mongo
  # Mongo documentation for query details.
  class Query

    attr_accessor :number_to_skip, :number_to_return, :order_by, :snapshot
    # If true, $explain will be set in QueryMessage that uses this query.
    attr_accessor :explain
    # Either +nil+ or a hash (preferably an OrderedHash).
    attr_accessor :hint
    attr_reader :selector   # writer defined below

    # sel :: A hash describing the query. See the Mongo docs for details.
    #
    # return_fields :: If not +nil+, a single field name or an array of
    #                  field names. Only those fields will be returned.
    #                  (Called :fields in calls to Collection#find.)
    #
    # number_to_skip :: Number of records to skip before returning
    #                   records. Default is 0.
    #
    # number_to_return :: Max number of records to return. (Called :limit
    #                     in calls to Collection#find.) Default is 0 (all
    #                     records).
    #
    # order_by :: If not +nil+, specifies record sort order. May be a
    #             String, Hash, OrderedHash, or Array. If a string, the
    #             results will be ordered by that field in ascending
    #             order. If an array, it should be an array of field names
    #             which will all be sorted in ascending order. If a hash,
    #             it may be either a regular Hash or an OrderedHash. The
    #             keys should be field names, and the values should be 1
    #             (ascending) or -1 (descending). Note that if it is a
    #             regular Hash then sorting by more than one field
    #             probably will not be what you intend because key order
    #             is not preserved. (order_by is called :sort in calls to
    #             Collection#find.)
    # :snapshot :: If true, snapshot mode will be used for this query.
    #              Snapshot mode assures no duplicates are returned, or
    #              objects missed, which were preset at both the start and
    #              end of the query's execution. For details see
    #              http://www.mongodb.org/display/DOCS/How+to+do+Snapshotting+in+the+Mongo+Database
    #
    # hint :: If not +nil+, specifies query hint fields. Must be either
    #         +nil+ or a hash (preferably an OrderedHash). See Collection#hint.
    #
    # timeout :: When +true+ (default), the returned cursor will be subject to 
    #             the normal cursor timeout behavior of the mongod process. 
    #             When +false+, the returned cursor will never timeout. Care should 
    #             be taken to ensure that cursors with timeout disabled are properly closed.
    def initialize(sel={}, return_fields=nil, number_to_skip=0, number_to_return=0, order_by=nil, hint=nil, snapshot=nil, timeout=true)
      @number_to_skip, @number_to_return, @order_by, @hint, @snapshot, @timeout =
        number_to_skip, number_to_return, order_by, hint, snapshot, timeout
      @explain = nil
      self.selector = sel
      self.fields = return_fields
    end

    # Set query selector hash. If sel is Code/string, it will be used as a
    # $where clause. (See Mongo docs for details.)
    def selector=(sel)
      @selector = case sel
                  when nil
                    {}
                  when Code
                    {"$where" => sel}
                  when String
                    {"$where" => Code.new(sel)}
                  when Hash
                    sel
                  end
    end

    # Set fields to return. If +val+ is +nil+ or empty, all fields will be
    # returned.
    def fields=(val)
      @fields = val
      @fields = nil if @fields && @fields.empty?
    end

    def fields
      case @fields
      when String
        {@fields => 1}
      when Array
        if @fields.length == 0
          nil
        else
          h = {}
          @fields.each { |field| h[field] = 1 }
          h
        end
      else                  # nil, anything else
        nil
      end
    end

    def contains_special_fields
      @order_by || @explain || @hint || @snapshot
    end

    # Returns an integer indicating which query options have been selected.
    # See http://www.mongodb.org/display/DOCS/Mongo+Wire+Protocol#MongoWireProtocol-OPQUERY
    def query_opts
      @timeout ? 0 : OP_QUERY_NO_CURSOR_TIMEOUT
    end

    def to_s
      "find(#{@selector.inspect})" + (@order_by ? ".sort(#{@order_by.inspect})" : "")
    end
  end
end
