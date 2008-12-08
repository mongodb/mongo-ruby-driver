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
require 'mongo/query'
require 'mongo/util/ordered_hash.rb'

module XGen
  module Mongo
    module Driver

      class DB
        SYSTEM_NAMESPACE_COLLECTION = "system.namespaces"
        SYSTEM_INDEX_COLLECTION = "system.indexes"
        SYSTEM_COMMAND_COLLECTION = "$cmd"

        attr_reader :name, :socket

        def initialize(db_name, host, port)
          raise "Invalid DB name" if !db_name || (db_name && db_name.length > 0 && db_name.include?("."))
          @name, @host, @port = db_name, host, port
          @socket = TCPSocket.new(@host, @port)
        end

        def collection_names
          names = collections_info.collect { |doc| doc['name'] || '' }
          names.delete('')
          names
        end

        def collections_info(coll_name=nil)
          selector = {}
          selector[:name] = "#{@name}.#{coll_name}" if coll_name
          query(SYSTEM_NAMESPACE_COLLECTION, Query.new(selector))
        end

        def create_collection(name, options={})
          # First check existence
          return Collection.new(self, name) if collection_names.include?(name)

          # Create new collection
          oh = OrderedHash.new
          oh[:create] = name
          doc = db_command(oh.merge(options))
          o = doc['ok']
          return Collection.new(self, name) if o.kind_of?(Numeric) && (o.to_i == 1 || o.to_i == 0)
          raise "Error creating collection: #{doc.inspect}"
        end

        def admin
          # TODO
          raise "not implemented"
          Admin.new(self)
        end

        def collection(name)
          # We do not implement the Java driver's optional strict mode, which
          # throws an exception if the collection does not exist.
          create_collection(name)
        end

        def drop_collection(name)
          coll = collection(name)
          return true if coll == nil
          coll.drop_indexes     # Mongo requires that we drop indexes manually

          doc = db_command(:drop => name)
          o = doc['ok']
          return o.kind_of?(Numeric) && o.to_i == 1
        end

        def close
          @socket.close
        end

        def send_message(msg)
          send_to_db(MsgMessage.new(msg))
        end
        
        def query(collection_name, query)
          # TODO synchronize
          send_to_db(QueryMessage.new(@name, collection_name, query))
          return Cursor.new(self, collection_name)
        end

        def remove_from_db(collection_name, selector)
          # TODO synchronize
          send_to_db(RemoveMessage.new(@name, collection_name, selector))
        end

        def replace_in_db(collection_name, selector, obj)
          # TODO synchronize
          send_to_db(UpdateMessage.new(@name, collection_name, selector, obj, false))
        end
        alias_method :modify_in_db, :replace_in_db

        def repsert_in_db(collection_name, selector, obj)
          # TODO if PKInjector, inject
          # TODO synchronize
          send_to_db(UpdateMessage.new(@name, collection_name, selector, obj, true))
          obj
        end

        def count(collection_name, selector)
          oh = OrderedHash.new
          oh[:count] = collection_name
          oh[:query] = selector
          doc = db_command(oh)
          o = doc['ok']
          return doc['n'].to_i if o.to_i == 1
          raise "Error with count command: #{doc.inspect}"
        end

        def drop_index(collection_name, name)
          oh = OrderedHash.new
          oh[:deleteIndexes] = collection_name
          oh[:index] = name
          doc = db_command(oh)
          o = doc['ok']
          raise "Error with drop_index command: #{doc.inspect}" unless o.kind_of?(Numeric) && o.to_i == 1
        end

        def index_information(collection_name)
          sel = {:ns => full_coll_name(collection_name)}
          # TODO synchronize
          query(SYSTEM_INDEX_COLLECTION, Query.new(sel)).collect { |row|
            h = {:name => row['name']}
            raise "Name of index on return from db was nil. Coll = #{full_coll_name(collection_name)}" unless h[:name]

            h[:keys] = row['key']
            raise "Keys for index on return from db was nil. Coll = #{full_coll_name(collection_name)}" unless h[:keys]

            h[:ns] = row['ns']
            raise "Namespace for index on return from db was nil. Coll = #{full_coll_name(collection_name)}" unless h[:ns]
            h[:ns].sub!(/.*\./, '')
            raise "Error: ns != collection" unless h[:ns] == collection_name

            h
          }
        end

        def create_index(collection_name, index_name, fields)
          sel = {:name => index_name, :ns => full_coll_name(collection_name)}
          field_h = {}
          fields.each { |f| field_h[f] = 1 }
          sel[:key] = field_h
          # TODO synchronize
          send_to_db(InsertMessage.new(@name, SYSTEM_INDEX_COLLECTION, sel))
        end

        def insert_into_db(collection_name, objects)
          # TODO synchronize
          objects.each { |o| send_to_db(InsertMessage.new(@name, collection_name, o)) }
        end

        def send_to_db(message)
          @socket.print(message.buf.to_s)
        end

        def full_coll_name(collection_name)
          "#{@name}.#{collection_name}"
        end

        protected

        # DB commands need to be ordered, so selector must be an OrderedHash
        # (or a Hash with only one element). What DB commands really need is
        # that the "command" key be first.
        def db_command(selector)
          if !selector.kind_of?(OrderedHash)
            if !selector.kind_of?(Hash) || selector.keys.length > 1
              raise "db_command must be given an OrderedHash when there is more than one key"
            end
          end

          # TODO synchronize
          q = Query.new(selector)
          q.number_to_return = 1
          query(SYSTEM_COMMAND_COLLECTION, q).next_object
        end

      end
    end
  end
end
