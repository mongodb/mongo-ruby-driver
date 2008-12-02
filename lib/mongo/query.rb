# Copyright (C) 2008 10gen Inc.
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

require 'socket'
require 'mongo/collection'
require 'mongo/message'

module XGen
  module Mongo
    module Driver

      class Query

        attr_accessor :number_to_skip, :number_to_return, :order_by
        attr_reader :selector, :fields # writers defined below

        def initialize(sel={}, return_fields=nil, number_to_skip=0, number_to_return=0, order_by=nil)
          @number_to_skip, @number_to_return, @order_by = number_to_skip, number_to_return, order_by
          self.selector = sel
          self.fields = return_fields
        end

        def selector=(sel)
          @selector = case sel
                      when nil
                        {}
                      when String
                        {"$where" => "function() { return #{sel}; }"}
                      when Hash
                        sel
                      end
        end

        def fields=(val)
          @fields = val
          @fields = nil if @fields && @fields.empty?
        end
      end
    end
  end
end
