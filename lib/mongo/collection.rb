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

require 'mongo/query'

module XGen
  module Mongo
    module Driver

      # A named collection of records in a database.
      class Collection

        attr_reader :db, :name, :hint

        def initialize(db, name)
          case name
          when Symbol, String
          else
            raise TypeError, "new_name must be a string or symbol"
          end

          name = name.to_s

          if name.empty? or name.include? ".."
            raise InvalidName, "collection names cannot be empty"
          end
          if name.include? "$" and not name.match(/^\$cmd/)
            raise InvalidName, "collection names must not contain '$'"
          end
          if name.match(/^\./) or name.match(/\.$/)
            raise InvalidName, "collection names must not start or end with '.'"
          end

          @db, @name = db, name
          @hint = nil
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
        # :snapshot :: If true, snapshot mode will be used for this query.
        #              Snapshot mode assures no duplicates are returned, or
        #              objects missed, which were preset at both the start and
        #              end of the query's execution. For details see
        #              http://www.mongodb.org/display/DOCS/How+to+do+Snapshotting+in+the+Mongo+Database
        def find(selector={}, options={})
          fields = options.delete(:fields)
          fields = nil if fields && fields.empty?
          offset = options.delete(:offset) || 0
          limit = options.delete(:limit) || 0
          sort = options.delete(:sort)
          hint = options.delete(:hint)
          snapshot = options.delete(:snapshot)
          if hint
            hint = normalize_hint_fields(hint)
          else
            hint = @hint        # assumed to be normalized already
          end
          raise RuntimeError, "Unknown options [#{options.inspect}]" unless options.empty?
          @db.query(self, Query.new(selector, fields, offset, limit, sort, hint, snapshot))
        end

        # Find the first record that matches +selector+. See #find.
        def find_first(selector={}, options={})
          h = options.dup
          h[:limit] = 1
          cursor = find(selector, h)
          cursor.next_object    # don't need to explicitly close b/c of limit
        end

        # Save a document in this collection
        #
        # If +to_save+ already has an '_id' then an update (upsert) operation
        # is performed and any existing document with that _id is overwritten.
        # Otherwise an insert operation is performed.
        #
        # :to_save :: the document (a hash) to be saved
        def save(to_save)
          if id = to_save[:_id] || to_save['_id']
            update({:_id => id}, to_save, :upsert => true)
            id
          else
            insert(to_save)
          end
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

        # DEPRECATED - use update(... :upsert => true) instead
        #
        # Update records that match +selector+ by applying +obj+ as an update.
        # If no match, inserts (???).
        def repsert(selector, obj)
          warn "Collection#repsert is deprecated and will be removed. Please use Collection#update instead."
          update(selector, obj, :upsert => true)
        end

        # DEPRECATED - use update(... :upsert => false) instead
        #
        # Update records that match +selector+ by applying +obj+ as an update.
        def replace(selector, obj)
          warn "Collection#replace is deprecated and will be removed. Please use Collection#update instead."
          update(selector, obj)
        end

        # DEPRECATED - use update(... :upsert => false) instead
        #
        # Update records that match +selector+ by applying +obj+ as an update.
        # Both +selector+ and +modifier_obj+ are required.
        def modify(selector, modifier_obj)
          warn "Collection#modify is deprecated and will be removed. Please use Collection#update instead."
          update(selector, modifier_obj)
        end

        # Update a document(s) in this collection.
        #
        # :spec :: a hash specifying elements which must be present for
        #   a document to be updated
        # :document :: a hash specifying the fields to be changed in the
        #   selected document(s), or (in the case of an upsert) the document to
        #   be inserted
        #
        # Options:
        # :upsert :: if true, perform an upsert operation
        # :safe :: if true, check that the update succeeded. OperationFailure
        #   will be raised on an error. Checking for safety requires an extra
        #   round-trip to the database
        def update(spec, document, options={})
          upsert = options.delete(:upsert)
          safe = options.delete(:safe)

          if upsert
            @db.repsert_in_db(@name, spec, document)
          else
            @db.replace_in_db(@name, spec, document)
          end
          if safe
            error = @db.error
            if error
              raise OperationFailure, error
            end
          end
        end

        # Create a new index. +field_or_spec+
        # should be either a single field name or a Array of [field name,
        # direction] pairs. Directions should be specified as
        # XGen::Mongo::ASCENDING or XGen::Mongo::DESCENDING.
        # +unique+ is an optional boolean indicating whether this index
        # should enforce a uniqueness constraint.
        def create_index(field_or_spec, unique=false)
          @db.create_index(@name, field_or_spec, unique)
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

        # Perform a query similar to an SQL group by operation.
        #
        # Returns an array of grouped items.
        #
        # :keys :: list of fields to group by
        # :condition :: specification of rows to be considered (as a 'find'
        #               query specification)
        # :initial :: initial value of the aggregation counter object
        # :reduce :: aggregation function as a JavaScript string
        def group(keys, condition, initial, reduce)
          group_function = <<EOS
function () {
    var c = db[ns].find(condition);
    var map = new Map();
    var reduce_function = #{reduce};
    while (c.hasNext()) {
        var obj = c.next();

        var key = {};
        for (var i in keys) {
            key[keys[i]] = obj[keys[i]];
        }

        var aggObj = map.get(key);
        if (aggObj == null) {
            var newObj = Object.extend({}, key);
            aggObj = Object.extend(newObj, initial);
            map.put(key, aggObj);
        }
        reduce_function(obj, aggObj);
    }
    return {"result": map.values()};
}
EOS
          return @db.eval(Code.new(group_function,
                                   {
                                     "ns" => @name,
                                     "keys" => keys,
                                     "condition" => condition,
                                     "initial" => initial
                                   }))["result"]
        end

        # Rename this collection.
        #
        # If operating in auth mode, client must be authorized as an admin to
        # perform this operation. Raises +InvalidName+ if +new_name+ is an invalid
        # collection name.
        #
        # :new_name :: new name for this collection
        def rename(new_name)
          case new_name
          when Symbol, String
          else
            raise TypeError, "new_name must be a string or symbol"
          end

          new_name = new_name.to_s

          if new_name.empty? or new_name.include? ".."
            raise InvalidName, "collection names cannot be empty"
          end
          if new_name.include? "$"
            raise InvalidName, "collection names must not contain '$'"
          end
          if new_name.match(/^\./) or new_name.match(/\.$/)
            raise InvalidName, "collection names must not start or end with '.'"
          end

          @db.rename_collection(@name, new_name)
        end

        # Get information on the indexes for the collection +collection_name+.
        # Returns a hash where the keys are index names (as returned by
        # Collection#create_index and the values are lists of [key, direction]
        # pairs specifying the index (as passed to Collection#create_index).
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

