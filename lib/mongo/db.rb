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
require 'mutex_m'
require 'mongo/mongo'
require 'mongo/collection'
require 'mongo/message'
require 'mongo/query'
require 'mongo/util/ordered_hash.rb'
require 'mongo/admin'

module XGen
  module Mongo
    module Driver

      # A Mongo database.
      class DB

        SYSTEM_NAMESPACE_COLLECTION = "system.namespaces"
        SYSTEM_INDEX_COLLECTION = "system.indexes"
        SYSTEM_PROFILE_COLLECTION = "system.profile"
        SYSTEM_COMMAND_COLLECTION = "$cmd"

        # Strict mode enforces collection existence checks. When +true+,
        # asking for a collection that does not exist or trying to create a
        # collection that already exists raises an error.
        #
        # Strict mode is off (+false+) by default. Its value can be changed at
        # any time.
        attr_writer :strict

        # Returns the value of the +strict+ flag.
        def strict?; @strict; end

        # The name of the database.
        attr_reader :name

        attr_reader :host, :port

        # The database's socket. For internal use only.
        attr_reader :socket

        # db_name :: The database name
        #
        # host :: The database host name or IP address. Defaults to 'localhost'.
        #
        # port :: The database port number. Defaults to
        #         XGen::Mongo::Driver::Mongo::DEFAULT_PORT.
        #
        def initialize(db_name, host='localhost', port=XGen::Mongo::Driver::Mongo::DEFAULT_PORT)
          raise "Invalid DB name" if !db_name || (db_name && db_name.length > 0 && db_name.include?("."))
          @name, @host, @port = db_name, host, port
          @socket = TCPSocket.new(@host, @port)
          @strict = false
          @semaphore = Object.new
          @semaphore.extend Mutex_m
        end

        # Returns an array of collection names. Each name is of the form
        # "database_name.collection_name".
        def collection_names
          names = collections_info.collect { |doc| doc['name'] || '' }
          names.delete('')
          names
        end

        # Returns a cursor over query result hashes. Each hash contains a
        # 'name' string and optionally an 'options' hash. If +coll_name+ is
        # specified, an array of length 1 is returned.
        def collections_info(coll_name=nil)
          selector = {}
          selector[:name] = full_coll_name(coll_name) if coll_name
          query(Collection.new(self, SYSTEM_NAMESPACE_COLLECTION), Query.new(selector))
        end

        # Create a collection. If +strict+ is false, will return existing or
        # new collection. If +strict+ is true, will raise an error if
        # collection +name+ already exists.
        #
        # Options is an optional hash:
        #
        # :capped :: Boolean. If not specified, capped is +false+.
        #
        # :size :: If +capped+ is +true+, specifies the maximum number of
        #          bytes. If +false+, specifies the initial extent of the
        #          collection.
        #
        # :max :: Max number of records in a capped collection. Optional.
        def create_collection(name, options={})
          # First check existence
          if collection_names.include?(full_coll_name(name))
            if strict?
              raise "Collection #{name} already exists. Currently in strict mode."
            else
              return Collection.new(self, name)
            end
          end

          # Create new collection
          oh = OrderedHash.new
          oh[:create] = name
          doc = db_command(oh.merge(options || {}))
          ok = doc['ok']
          return Collection.new(self, name) if ok.kind_of?(Numeric) && (ok.to_i == 1 || ok.to_i == 0)
          raise "Error creating collection: #{doc.inspect}"
        end

        def admin
          Admin.new(self)
        end

        # Return a collection. If +strict+ is false, will return existing or
        # new collection. If +strict+ is true, will raise an error if
        # collection +name+ does not already exists.
        def collection(name)
          return Collection.new(self, name) if collection_names.include?(full_coll_name(name))
          if strict?
            raise "Collection #{name} doesn't exist. Currently in strict mode."
          else
            create_collection(name)
          end
        end

        # Drop collection +name+. Returns +true+ on success or if the
        # collection does not exist, +false+ otherwise.
        def drop_collection(name)
          return true unless collection_names.include?(full_coll_name(name))

          coll = collection(name)
          coll.drop_indexes     # Mongo requires that we drop indexes manually
          ok?(db_command(:drop => name))
        end

        # Returns true if this database is a master (or is not paired with any
        # other database), false if it is a slave.
        def master?
          doc = db_command(:ismaster => 1)
          is_master = doc['ismaster']
          ok?(doc) && is_master.kind_of?(Numeric) && is_master.to_i == 1
        end

        # Returns a string of the form "host:port" that points to the master
        # database. Works even if this is the master database.
        def master
          doc = db_command(:ismaster => 1)
          is_master = doc['ismaster']
          raise "Error retrieving master database" unless ok?(doc) && is_master.kind_of?(Numeric)
          case is_master.to_i
          when 1
            "#@host:#@port"
          else
            doc['remote']
          end
        end

        # Close the connection to the database.
        def close
          @socket.close
        end

        # Send a MsgMessage to the database.
        def send_message(msg)
          send_to_db(MsgMessage.new(msg))
        end
        
        # Returns a Cursor over the query results.
        #
        # Note that the query gets sent lazily; the cursor calls
        # #send_query_message when needed. If the caller never requests an
        # object from the cursor, the query never gets sent.
        def query(collection, query)
          Cursor.new(self, collection, QueryMessage.new(@name, collection.name, query))
        end

        # Used by a Cursor to lazily send the query to the database.
        def send_query_message(query_message)
          @semaphore.synchronize {
            send_to_db(query_message)
          }
        end

        # Remove the records that match +selector+ from +collection_name+.
        # Normally called by Collection#remove or Collection#clear.
        def remove_from_db(collection_name, selector)
          @semaphore.synchronize {
            send_to_db(RemoveMessage.new(@name, collection_name, selector))
          }
        end

        # Update records in +collection_name+ that match +selector+ by
        # applying +obj+ as an update. Normally called by Collection#replace.
        def replace_in_db(collection_name, selector, obj)
          @semaphore.synchronize {
            send_to_db(UpdateMessage.new(@name, collection_name, selector, obj, false))
          }
        end

        # Alias for #replace_in_db. Normally called by Collection.modify.
        alias_method :modify_in_db, :replace_in_db

        # Update records in +collection_name+ that match +selector+ by
        # applying +obj+ as an update. If no match, inserts (???). Normally
        # called by Collection#repsert.
        def repsert_in_db(collection_name, selector, obj)
          # TODO if PKInjector, inject
          @semaphore.synchronize {
            send_to_db(UpdateMessage.new(@name, collection_name, selector, obj, true))
            obj
          }
        end

        # Return the number of records in +collection_name+ that match
        # +selector+. If +selector+ is +nil+ or an empty hash, returns the
        # count of all records. Normally called by Collection#count.
        def count(collection_name, selector={})
          oh = OrderedHash.new
          oh[:count] = collection_name
          oh[:query] = selector || {}
          doc = db_command(oh)
          return doc['n'].to_i if ok?(doc)
          raise "Error with count command: #{doc.inspect}"
        end

        # Drop index +name+ from +collection_name+. Normally called from
        # Collection#drop_index or Collection#drop_indexes.
        def drop_index(collection_name, name)
          oh = OrderedHash.new
          oh[:deleteIndexes] = collection_name
          oh[:index] = name
          doc = db_command(oh)
          raise "Error with drop_index command: #{doc.inspect}" unless ok?(doc)
        end

        # Return an array of hashes, one for each index on +collection_name+.
        # Normally called by Collection#index_information. Each hash contains:
        #
        # :name :: Index name
        #
        # :keys :: Hash whose keys are the names of the fields that make up
        #          the key and values are integers.
        #
        # :ns :: Namespace; same as +collection_name+.
        def index_information(collection_name)
          sel = {:ns => full_coll_name(collection_name)}
          query(Collection.new(self, SYSTEM_INDEX_COLLECTION), Query.new(sel)).collect { |row|
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

        # Create a new index on +collection_name+ named +index_name+. +fields+
        # should be an array of field names. Normally called by
        # Collection#create_index.
        def create_index(collection_name, index_name, fields)
          sel = {:name => index_name, :ns => full_coll_name(collection_name)}
          field_h = {}
          fields.each { |f| field_h[f] = 1 }
          sel[:key] = field_h
          @semaphore.synchronize {
            send_to_db(InsertMessage.new(@name, SYSTEM_INDEX_COLLECTION, sel))
          }
        end

        # Insert +objects+ into +collection_name+. Normally called by
        # Collection#insert.
        def insert_into_db(collection_name, objects)
          @semaphore.synchronize {
            objects.each { |o| send_to_db(InsertMessage.new(@name, collection_name, o)) }
          }
        end

        def send_to_db(message)
          @socket.print(message.buf.to_s)
        end

        def full_coll_name(collection_name)
          "#{@name}.#{collection_name}"
        end

        # Return +true+ if +doc+ contains an 'ok' field with the value 1.
        def ok?(doc)
          ok = doc['ok']
          ok.kind_of?(Numeric) && ok.to_i == 1
        end

        # DB commands need to be ordered, so selector must be an OrderedHash
        # (or a Hash with only one element). What DB commands really need is
        # that the "command" key be first.
        #
        # Do not call this. Intended for driver use only.
        def db_command(selector)
          if !selector.kind_of?(OrderedHash)
            if !selector.kind_of?(Hash) || selector.keys.length > 1
              raise "db_command must be given an OrderedHash when there is more than one key"
            end
          end

          q = Query.new(selector)
          q.number_to_return = 1
          query(Collection.new(self, SYSTEM_COMMAND_COLLECTION), q).next_object
        end

      end
    end
  end
end
