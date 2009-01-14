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
          write_int(query.number_to_return)
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
                                 query.order_by.each { |ob| h[ob] = 1 }
                                 h
                               when Hash # Should be an ordered hash, but this message doesn't care
                                 query.order_by
                               else
                                 raise "illegal order_by: is a #{query.order_by.class.name}, must be String, Array, Hash, or OrderedHash"
                               end
            end
            if query.hint_fields && query.hint_fields.length > 0
              hints = OrderedHash.new
              query.hint_fields.each { |hf| hints[hf] = 1 }
              sel['$hint'] = hints
            end
            if query.explain
              sel['$explain'] = true
            end
                               
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
