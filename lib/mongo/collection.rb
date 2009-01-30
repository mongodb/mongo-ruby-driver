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

require 'mongo/query'

module XGen
  module Mongo
    module Driver

      # A named collection of records in a database.
      class Collection

        attr_reader :db, :name, :hint

        def initialize(db, name)
          @db = db
          @name = name
        end

        # Set hint fields to use and return +self+. hint may be a single field
        # name, array of field names, or a hash (preferably an OrderedHash).
        # May be +nil+.
        def hint=(hint)
          @hint = normalize_hint_fields(hint)
          self
        end

        # Return records that match a +selector+ hash. See Mongo docs for
        # details.
        #
        # Options:
        # :fields :: Array of collection field names; only those will be returned (plus _id if defined)
        # :offset :: Start at this record when returning records
        # :limit :: Maximum number of records to return
        # :sort :: Either hash of field names as keys and 1/-1 as values; 1 ==
        #          ascending, -1 == descending, or array of field names (all
        #          assumed to be sorted in ascending order).
        # :hint :: See #hint. This option overrides the collection-wide value.
        def find(selector={}, options={})
          fields = options.delete(:fields)
          fields = nil if fields && fields.empty?
          offset = options.delete(:offset) || 0
          limit = options.delete(:limit) || 0
          sort = options.delete(:sort)
          hint = options.delete(:hint)
          if hint
            hint = normalize_hint_fields(hint)
          else
            hint = @hint        # assumed to be normalized already
          end
          raise RuntimeError, "Unknown options [#{options.inspect}]" unless options.empty?
          @db.query(self, Query.new(selector, fields, offset, limit, sort, hint))
        end

        # Insert +objects+, which are hashes. "<<" is aliased to this method.
        # Returns either the single inserted object or a new array containing
        # +objects+. The object(s) may have been modified by the database's PK
        # factory, if it has one.
        def insert(*objects)
          objects = objects.first if objects.size == 1 && objects.first.is_a?(Array)
          res = @db.insert_into_db(@name, objects)
          res.size > 1 ? res : res.first
        end
        alias_method :<<, :insert

        # Remove the records that match +selector+.
        def remove(selector={})
          @db.remove_from_db(@name, selector)
        end

        # Remove all records.
        def clear
          remove({})
        end

        # Update records that match +selector+ by applying +obj+ as an update.
        # If no match, inserts (???).
        def repsert(selector, obj)
          @db.repsert_in_db(@name, selector, obj)
        end

        # Update records that match +selector+ by applying +obj+ as an update.
        def replace(selector, obj)
          @db.replace_in_db(@name, selector, obj)
        end

        # Update records that match +selector+ by applying +obj+ as an update.
        # Both +selector+ and +modifier_obj+ are required.
        def modify(selector, modifier_obj)
          raise "no object" unless modifier_obj
          raise "no selector" unless selector
          @db.modify_in_db(@name, selector, modifier_obj)
        end

        # Create a new index named +index_name+. +fields+ should be an array
        # of field names.
        def create_index(name, *fields)
          @db.create_index(@name, name, fields)
        end

        # Drop index +name+.
        def drop_index(name)
          @db.drop_index(@name, name)
        end

        # Drop all indexes.
        def drop_indexes
          # just need to call drop indexes with no args; will drop them all
          @db.drop_index(@name, '*')
        end

        # Drop the entire collection. USE WITH CAUTION.
        def drop
          @db.drop_collection(@name)
        end

        # Return an array of hashes, one for each index. Each hash contains:
        #
        # :name :: Index name
        #
        # :keys :: Hash whose keys are the names of the fields that make up
        #          the key and values are integers.
        #
        # :ns :: Namespace; same as this collection's name.
        def index_information
          @db.index_information(@name)
        end

        # Return a hash containing options that apply to this collection.
        # 'create' will be the collection name. For the other possible keys
        # and values, see DB#create_collection.
        def options
          @db.collections_info(@name).next_object()['options']
        end

        # Return the number of records that match +selector+. If +selector+ is
        # +nil+ or an empty hash, returns the count of all records.
        def count(selector={})
          @db.count(@name, selector || {})
        end

        protected

        def normalize_hint_fields(hint)
          case hint
          when String
            {hint => 1}
          when Hash
            hint
          when nil
            nil
          else
            h = OrderedHash.new
            hint.to_a.each { |k| h[k] = 1 }
            h
          end
        end
      end
    end
  end
end

