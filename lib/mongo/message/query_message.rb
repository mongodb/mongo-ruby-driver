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
require 'mongo/util/conversions'
require 'mongo/util/ordered_hash'

module Mongo
  class QueryMessage < Message
    include Mongo::Conversions

    attr_reader :query

    def initialize(db_name, collection_name, query)
      super(OP_QUERY)
      @query = query
      @collection_name = collection_name
      write_int(0)
      write_string("#{db_name}.#{collection_name}")
      write_int(query.number_to_skip)
      write_int(query.number_to_return)
      sel = query.selector
      if query.contains_special_fields
        sel = OrderedHash.new
        sel['query'] = query.selector
        if query.order_by
          order_by = query.order_by
          sel['orderby'] = case order_by
                           when String then string_as_sort_parameters(order_by)
                           when Symbol then symbol_as_sort_parameters(order_by)
                           when Array then array_as_sort_parameters(order_by)
                           when Hash # Should be an ordered hash, but this message doesn't care
                             warn_if_deprecated(order_by)
                             order_by
                           else
                             raise InvalidSortValueError.new("illegal order_by: is a #{query.order_by.class.name}, must be String, Array, Hash, or OrderedHash")
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

    def to_s
      "db.#{@collection_name}.#{@query}"
    end
  end
end
