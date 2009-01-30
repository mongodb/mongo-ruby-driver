# --
# Copyright (C) 2008-2009 10gen Inc.
#
# This program is free software: you can redistribute it and/or modify it
# under the terms of the GNU Affero General Public License, version 3, as
# published by the Free Software Foundation.
#
# This program is distributed in the hope that it will be useful, but WITHOUT
# ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
# FITNESS FOR A PARTICULAR PURPOSE. See the GNU Affero General Public License
# for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program. If not, see <http://www.gnu.org/licenses/>.
# ++

require 'mongo/collection'
require 'mongo/message'

module XGen
  module Mongo
    module Driver

      # A query against a collection. A query's selector is a hash. See the
      # Mongo documentation for query details.
      class Query

        attr_accessor :number_to_skip, :number_to_return, :order_by
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
        def initialize(sel={}, return_fields=nil, number_to_skip=0, number_to_return=0, order_by=nil, hint=nil)
          @number_to_skip, @number_to_return, @order_by, @hint =
            number_to_skip, number_to_return, order_by, hint
          self.selector = sel
          self.fields = return_fields
        end

        # Set query selector hash. If sel is a string, it will be used as a
        # $where clause. (See Mongo docs for details.)
        def selector=(sel)
          @selector = case sel
                      when nil
                        {}
                      when String
                        {"$where" => sel}
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
          (@order_by != nil && @order_by.length > 0) || @explain || @hint
        end
      end
    end
  end
end
