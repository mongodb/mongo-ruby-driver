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

module XGen
  module Mongo
    module Driver

      # A query against a collection. A query's selector is a hash. See the
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
        #                   records. (Called :offset in calls to
        #                   Collection#find.) Default is 0.
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
        #
        # hint :: If not +nil+, specifies query hint fields. Must be either
        #                +nil+ or a hash (preferably an OrderedHash). See
        #                Collection#hint.
        def initialize(sel={}, return_fields=nil, number_to_skip=0, number_to_return=0, order_by=nil, hint=nil, snapshot=nil)
          @number_to_skip, @number_to_return, @order_by, @hint, @snapshot =
            number_to_skip, number_to_return, order_by, hint, snapshot
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
          (@order_by != nil && @order_by.length > 0) || @explain || @hint || @snapshot
        end
      end
    end
  end
end
