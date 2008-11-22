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
          sel = {:create => name}.merge(options)
          doc = db_command(sel)
          o = doc['ok']
          return Collection.new(self, name) if o.kind_of?(Numeric) && (o.to_i == 1 || o.to_i == 0)
          raise "Error creating collection: #{doc.inspect}"
        end

        def admin
          Admin.new(self)
        end

        def collection(name)
          create_collection(name)
        end

        def drop_collection(name)
          coll = collection(name)
          return true if coll == nil
          col.drop_indexes

          doc = db_command(:drop => name)
          o = md['ok']
          return o.kind_of?(Numeric) && o.to_i == 1
        end

        def close
          @socket.close
        end

        def send_message(msg)
          send_to_db(MsgMessage.new(msg))
        end
        
        def query(collection, query)
          # TODO synchronize
          send_to_db(QueryMessage.new(@name, collection, query))
          return Cursor.new(self, collection)
        end

        def remove_from_db(collection, selector)
          # TODO synchronize
          send_to_db(RemoveMessage.new(@name, collection, selector))
        end

        def replace_in_db(collection, selector, obj)
          # TODO synchronize
          send_to_db(UpdateMessage.new(@name, collection, selector, obj, false))
        end
        alias_method :modify_in_db, :replace_in_db

        def repsert_in_db(collection, selector, obj)
          # TODO if PKInjector, inject
          # TODO synchronize
          send_to_db(UpdateMessage.new(@name, collection, selector, obj, true))
          obj
        end

        def count(collection, selector)
          doc = db_command(:count => collection, :query => selector)
          o = doc['ok']
          return doc['n'].to_i if o.to_i == 1
          raise "Error with count command: #{doc.to_s}" unless o.kind_of?(Numeric)
        end

        def drop_index(collection, name)
          db_command(:deleteIndexes => collection, :index => name)
        end

        def index_information(collection)
          sel = {:ns => full_coll_name(collection)}
          # TODO synchronize
          query(SYSTEM_INDEX_COLLECTION, Query.new(sel)).collect { |row|
            h = {:name => row['name']}
            raise "Name of index on return from db was nil. Coll = #{full_coll_name(collection)}" unless h[:name]

            h[:keys] = row['keys']
            raise "Keys for index on return from db was nil. Coll = #{full_coll_name(collection)}" unless h[:keys]

            h[:ns] = row['ns']
            raise "Namespace for index on return from db was nil. Coll = #{full_coll_name(collection)}" unless h[:ns]
            h[:ns].sub!(/.*\./, '')
            raise "Error: ns != collection" unless h[:ns] == collection

            h
          }
        end

        def create_index(collection, name, fields)
          sel = {:name => name, :ns => full_coll_name(collection)}
          field_h = {}
          fields.each { |f| field_h[f] = 1 }
          sel['key'] = field_h
          # TODO synchronize
          send_to_db(InsertMessage.new(@name, SYSTEM_INDEX_COLLECTION, sel))
        end

        def insert_into_db(collection, objects)
          # TODO synchronize
          objects.each { |o| send_to_db(InsertMessage.new(@name, collection, o)) }
        end

        def send_to_db(message)
          @socket.print(message.buf.to_s)
        end

        protected

        def full_coll_name(collection)
          "#{@name}.#{collection}"
        end

        def db_command(selector)
          # TODO synchronize
          q = Query.new(selector)
          q.number_to_return = 1
          query(SYSTEM_COMMAND_COLLECTION, q).next_object
        end

      end
    end
  end
end

