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

require 'socket'
require 'mongo/collection'
require 'mongo/message'

module XGen
  module Mongo
    module Driver

      # A query against a collection. A query's selector is a hash. See the
      # Mongo documentation for query details.
      class Query

        attr_accessor :number_to_skip, :number_to_return, :order_by
        attr_reader :selector, :fields # writers defined below

        # sel :: A hash describing the query. See the Mongo docs for details.
        #
        # return_fields :: If not +nil+, an array of field names. Only those
        #                  fields will be returned. (Called :fields in calls
        #                  to Collection#find.)
        #
        # number_to_skip :: Number of records to skip before returning
        #                   records. (Called :offset in calls to
        #                   Collection#find.)
        #
        # number_to_return :: Max number of records to return. (Called :limit
        #                     in calls to Collection#find.)
        #
        # order_by :: If not +nil+, specifies record return order. Either hash
        #             of field names as keys and 1/-1 as values; 1 ==
        #             ascending, -1 == descending, or array of field names
        #             (all assumed to be sorted in ascending order). (Called
        #             :sort in calls to Collection#find.)
        def initialize(sel={}, return_fields=nil, number_to_skip=0, number_to_return=0, order_by=nil)
          @number_to_skip, @number_to_return, @order_by = number_to_skip, number_to_return, order_by
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
      end
    end
  end
end
