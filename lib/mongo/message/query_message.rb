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

require 'mongo/message/message'
require 'mongo/message/opcodes'
require 'mongo/util/ordered_hash'

module XGen
  module Mongo
    module Driver

      class QueryMessage < Message

        attr_reader :query

        def initialize(db_name, collection_name, query)
          super(OP_QUERY)
          @query = query
          write_int(0)
          write_string("#{db_name}.#{collection_name}")
          write_int(query.number_to_skip)
          write_int(-query.number_to_return) # Negative means hard limit
          sel = query.selector
          if query.contains_special_fields
            sel = OrderedHash.new
            sel['query'] = query.selector
            if query.order_by && query.order_by.length > 0
              sel['orderby'] = case query.order_by
                               when String
                                 {query.order_by => 1}
                               when Array
                                 h = OrderedHash.new
                                 query.order_by.each { |ob|
                                   case ob
                                   when String
                                     h[ob] = 1
                                   when Hash # should have one entry; will handle all
                                     ob.each { |k,v| h[k] = v }
                                   else
                                     raise "illegal query order_by value #{query.order_by.inspect}"
                                   end
                                 }
                                 h
                               when Hash # Should be an ordered hash, but this message doesn't care
                                 query.order_by
                               else
                                 raise "illegal order_by: is a #{query.order_by.class.name}, must be String, Array, Hash, or OrderedHash"
                               end
            end
            sel['$hint'] = query.hint if query.hint && query.hint.length > 0
            sel['$explain'] = true if query.explain
            sel['$snapshot'] = true if query.snapshot
          end
          write_doc(sel)
          write_doc(query.fields) if query.fields
        end

        def first_key(key)
          @first_key = key
        end
      end
    end
  end
end
