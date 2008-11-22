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

require 'mongo/query'

module XGen
  module Mongo
    module Driver
      class Collection

        attr_reader :db, :name

        def initialize(db, name)
          @db = db
          @name = name
        end

        def find(selector={}, fields=nil, options={})
          fields = nil if fields && fields.empty?
          @db.query(@name, Query.new(selector, fields, options[:offset] || 0, options[:limit] || 0, options[:sort]))
        end

        def insert(*objects)
          @db.insert_into_db(@name, objects)
        end

        def remove(selector={})
          @db.remove_from_db(@name, selector)
        end

        def clear
          remove({})
        end

        def repsert(selector, obj)
          @db.repsert_in_db(@name, selector, obj)
        end

        def replace(selector, obj)
          @db.replace_in_db(@name, selector, obj)
        end

        def modify(selector, modifierObj)
          raise "no object" unless modifierObj
          raise "no selector" unless selector
          @db.modify_in_db(@name, selector, modifierObj)
        end

        def create_index(name, *fields)
          @db.create_index(@name, name, fields)
        end

        def drop_index(name)
          @db.drop_index(@name, name)
        end

        def drop_indexes
          index_information.each { |info| @db.drop_index(@name, info.name) }
        end

        def index_information
          @db.index_information(@name)
        end

        def count(selector={})
          @db.count(@name, selector || {})
        end

      end
    end
  end
end

