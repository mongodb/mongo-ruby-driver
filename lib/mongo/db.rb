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

require 'socket'
require 'digest/md5'
require 'mutex_m'
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
        SYSTEM_USER_COLLECTION = "system.users"
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

        # Host to which we are currently connected.
        attr_reader :host
        # Port to which we are currently connected.
        attr_reader :port

        # An array of [host, port] pairs.
        attr_reader :nodes

        # The database's socket. For internal (and Cursor) use only.
        attr_reader :socket

        def slave_ok?; @slave_ok; end
        def auto_reconnect?; @auto_reconnect; end

        # A primary key factory object (or +nil+). See the README.doc file or
        # DB#new for details.
        attr_reader :pk_factory

        def pk_factory=(pk_factory)
          raise "error: can not change PK factory" if @pk_factory
          @pk_factory = pk_factory
        end

        # Instances of DB are normally obtained by calling Mongo#db.
        #
        # db_name :: The database name
        #
        # nodes :: An array of [host, port] pairs. See Mongo#new, which offers
        #          a more flexible way of defining nodes.
        #
        # options :: A hash of options.
        #
        # Options:
        #
        # :strict :: If true, collections must exist to be accessed and must
        #            not exist to be created. See #collection and
        #            #create_collection.
        #
        # :pk :: A primary key factory object that must respond to :create_pk,
        #        which should take a hash and return a hash which merges the
        #        original hash with any primary key fields the factory wishes
        #        to inject. (NOTE: if the object already has a primary key,
        #        the factory should not inject a new key; this means that the
        #        object is being used in a repsert but it already exists.) The
        #        idea here is that when ever a record is inserted, the :pk
        #        object's +create_pk+ method will be called and the new hash
        #        returned will be inserted.
        #
        # :slave_ok :: Only used if +nodes+ contains only one host/port. If
        #              false, when connecting to that host/port we check to
        #              see if the server is the master. If it is not, an error
        #              is thrown.
        #
        # :auto_reconnect :: If the connection gets closed (for example, we
        #                    have a server pair and saw the "not master"
        #                    error, which closes the connection), then
        #                    automatically try to reconnect to the master or
        #                    to the single server we have been given. Defaults
        #                    to +false+.
        #
        # When a DB object first connects to a pair, it will find the master
        # instance and connect to that one. On socket error or if we recieve a
        # "not master" error, we again find the master of the pair.
        def initialize(db_name, nodes, options={})
          case db_name
          when Symbol, String
          else
            raise TypeError, "db_name must be a string or symbol"
          end

          [" ", ".", "$", "/", "\\"].each do |invalid_char|
            if db_name.include? invalid_char
              raise InvalidName, "database names cannot contain the character '#{invalid_char}'"
            end
          end
          if db_name.empty?
            raise InvalidName, "database name cannot be the empty string"
          end

          @name, @nodes = db_name, nodes
          @strict = options[:strict]
          @pk_factory = options[:pk]
          @slave_ok = options[:slave_ok] && @nodes.length == 1 # only OK if one node
          @auto_reconnect = options[:auto_reconnect]
          @semaphore = Object.new
          @semaphore.extend Mutex_m
          @socket = nil
          connect_to_master
        end

        def connect_to_master
          close if @socket
          @host = @port = nil
          @nodes.detect { |hp|
            @host, @port = *hp
            begin
              @socket = TCPSocket.new(@host, @port)
              @socket.setsockopt(Socket::IPPROTO_TCP, Socket::TCP_NODELAY, 1)

              # Check for master. Can't call master? because it uses mutex,
              # which may already be in use during this call.
              semaphore_is_locked = @semaphore.locked?
              @semaphore.unlock if semaphore_is_locked
              is_master = master?
              @semaphore.lock if semaphore_is_locked

              break if @slave_ok || is_master
            rescue SocketError, SystemCallError, IOError => ex
              close if @socket
            end
            @socket
          }
          raise "error: failed to connect to any given host:port" unless @socket
        end

        # Returns true if +username+ has +password+ in
        # +SYSTEM_USER_COLLECTION+. +name+ is username, +password+ is
        # plaintext password.
        def authenticate(username, password)
          doc = db_command(:getnonce => 1)
          raise "error retrieving nonce: #{doc}" unless ok?(doc)
          nonce = doc['nonce']

          auth = OrderedHash.new
          auth['authenticate'] = 1
          auth['user'] = username
          auth['nonce'] = nonce
          auth['key'] = Digest::MD5.hexdigest("#{nonce}#{username}#{hash_password(username, password)}")
          ok?(db_command(auth))
        end

        # Deauthorizes use for this database for this connection.
        def logout
          doc = db_command(:logout => 1)
          raise "error logging out: #{doc.inspect}" unless ok?(doc)
        end

        # Returns an array of collection names in this database.
        def collection_names
          names = collections_info.collect { |doc| doc['name'] || '' }
          names = names.delete_if {|name| name.index(@name).nil? || name.index('$')}
          names.map {|name| name.sub(@name + '.', '')}
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
          if collection_names.include?(name)
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
          return Collection.new(self, name) if !strict? || collection_names.include?(name)
          raise "Collection #{name} doesn't exist. Currently in strict mode."
        end

        # Drop collection +name+. Returns +true+ on success or if the
        # collection does not exist, +false+ otherwise.
        def drop_collection(name)
          return true unless collection_names.include?(name)

          ok?(db_command(:drop => name))
        end

        # Returns the error message from the most recently executed database
        # operation for this connection, or +nil+ if there was no error.
        #
        # Note: as of this writing, errors are only detected on the db server
        # for certain kinds of operations (writes). The plan is to change this
        # so that all operations will set the error if needed.
        def error
          doc = db_command(:getlasterror => 1)
          raise "error retrieving last error: #{doc}" unless ok?(doc)
          doc['err']
        end

        # Returns +true+ if an error was caused by the most recently executed
        # database operation.
        #
        # Note: as of this writing, errors are only detected on the db server
        # for certain kinds of operations (writes). The plan is to change this
        # so that all operations will set the error if needed.
        def error?
          error != nil
        end

        # Get the most recent error to have occured on this database
        #
        # Only returns errors that have occured since the last call to
        # DB#reset_error_history - returns +nil+ if there is no such error.
        def previous_error
          error = db_command(:getpreverror => 1)
          if error["err"]
            error
          else
            nil
          end
        end

        # Reset the error history of this database
        #
        # Calls to DB#previous_error will only return errors that have occurred
        # since the most recent call to this method.
        def reset_error_history
          db_command(:reseterror => 1)
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
          raise "Error retrieving master database: #{doc.inspect}" unless ok?(doc) && is_master.kind_of?(Numeric)
          case is_master.to_i
          when 1
            "#@host:#@port"
          else
            doc['remote']
          end
        end

        # Close the connection to the database.
        def close
          if @socket
            s = @socket
            @socket = nil
            s.close
          end
        end

        def connected?
          @socket != nil
        end

        def receive_full(length)
          message = ""
          while message.length < length do
            chunk = @socket.recv(length - message.length)
            raise "connection closed" unless chunk.length > 0
            message += chunk
          end
          message
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
        def query(collection, query, admin=false)
          Cursor.new(self, collection, query, admin)
        end

        # Used by a Cursor to lazily send the query to the database.
        def send_query_message(query_message)
          send_to_db(query_message)
        end

        # Remove the records that match +selector+ from +collection_name+.
        # Normally called by Collection#remove or Collection#clear.
        def remove_from_db(collection_name, selector)
          _synchronize {
            send_to_db(RemoveMessage.new(@name, collection_name, selector))
          }
        end

        # Update records in +collection_name+ that match +selector+ by
        # applying +obj+ as an update. Normally called by Collection#replace.
        def replace_in_db(collection_name, selector, obj)
          _synchronize {
            send_to_db(UpdateMessage.new(@name, collection_name, selector, obj, false))
          }
        end

        # DEPRECATED - use Collection#update instead
        def modify_in_db(collection_name, selector, obj)
          warn "DB#modify_in_db is deprecated and will be removed. Please use Collection#update instead."
          replace_in_db(collection_name, selector, obj)
        end

        # Update records in +collection_name+ that match +selector+ by
        # applying +obj+ as an update. If no match, inserts (???). Normally
        # called by Collection#repsert.
        def repsert_in_db(collection_name, selector, obj)
          _synchronize {
            obj = @pk_factory.create_pk(obj) if @pk_factory
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
          return 0 if doc['errmsg'] == "ns missing"
          raise "Error with count command: #{doc.inspect}"
        end

        # Dereference a DBRef, getting the document it points to.
        def dereference(dbref)
          collection(dbref.namespace).find_one("_id" => dbref.object_id)
        end

        # Evaluate a JavaScript expression on MongoDB.
        # +code+ should be a string or Code instance containing a JavaScript
        # expression. Additional arguments will be passed to that expression
        # when it is run on the server.
        def eval(code, *args)
          if not code.is_a? Code
            code = Code.new(code)
          end

          oh = OrderedHash.new
          oh[:$eval] = code
          oh[:args] = args
          doc = db_command(oh)
          return doc['retval'] if ok?(doc)
          raise "Error with eval command: #{doc.inspect}"
        end

        # Rename collection +from+ to +to+. Meant to be called by
        # Collection#rename.
        def rename_collection(from, to)
          oh = OrderedHash.new
          oh[:renameCollection] = "#{@name}.#{from}"
          oh[:to] = "#{@name}.#{to}"
          doc = db_command(oh, true)
          raise "Error renaming collection: #{doc.inspect}" unless ok?(doc)
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

        # Get information on the indexes for the collection +collection_name+.
        # Normally called by Collection#index_information. Returns a hash where
        # the keys are index names (as returned by Collection#create_index and
        # the values are lists of [key, direction] pairs specifying the index
        # (as passed to Collection#create_index).
        def index_information(collection_name)
          sel = {:ns => full_coll_name(collection_name)}
          info = {}
          query(Collection.new(self, SYSTEM_INDEX_COLLECTION), Query.new(sel)).each { |index|
            info[index['name']] = index['key'].to_a
          }
          info
        end

        # Create a new index on +collection_name+. +field_or_spec+
        # should be either a single field name or a Array of [field name,
        # direction] pairs. Directions should be specified as
        # XGen::Mongo::ASCENDING or XGen::Mongo::DESCENDING. Normally called
        # by Collection#create_index. If +unique+ is true the index will
        # enforce a uniqueness constraint.
        def create_index(collection_name, field_or_spec, unique=false)
          field_h = OrderedHash.new
          if field_or_spec.is_a?(String) || field_or_spec.is_a?(Symbol)
            field_h[field_or_spec.to_s] = 1
          else
            field_or_spec.each { |f| field_h[f[0].to_s] = f[1] }
          end
          name = gen_index_name(field_h)
          sel = {
            :name => name,
            :ns => full_coll_name(collection_name),
            :key => field_h,
            :unique => unique
          }
          _synchronize {
            send_to_db(InsertMessage.new(@name, SYSTEM_INDEX_COLLECTION, false, sel))
          }
          name
        end

        # Insert +objects+ into +collection_name+. Normally called by
        # Collection#insert. Returns a new array containing the _ids
        # of the inserted documents.
        def insert_into_db(collection_name, objects)
          _synchronize {
            if @pk_factory
              objects.collect! { |o|
                @pk_factory.create_pk(o)
              }
            else
              objects = objects.collect do |o|
                o[:_id] || o['_id'] ? o : o.merge(:_id => ObjectID.new)
              end
            end
            send_to_db(InsertMessage.new(@name, collection_name, true, *objects))
            objects.collect { |o| o[:_id] || o['_id'] }
          }
        end

        def send_to_db(message)
          connect_to_master if !connected? && @auto_reconnect
          begin
            @socket.print(message.buf.to_s)
            @socket.flush
          rescue => ex
            close
            raise ex
          end
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
        def db_command(selector, use_admin_db=false)
          if !selector.kind_of?(OrderedHash)
            if !selector.kind_of?(Hash) || selector.keys.length > 1
              raise "db_command must be given an OrderedHash when there is more than one key"
            end
          end

          q = Query.new(selector)
          q.number_to_return = 1
          query(Collection.new(self, SYSTEM_COMMAND_COLLECTION), q, use_admin_db).next_object
        end

        def _synchronize &block
          @semaphore.synchronize &block
        end

        private

        def hash_password(username, plaintext)
          Digest::MD5.hexdigest("#{username}:mongo:#{plaintext}")
        end

        def gen_index_name(spec)
          temp = []
          spec.each_pair { |field, direction|
            temp = temp.push("#{field}_#{direction}")
          }
          return temp.join("_")
        end
      end
    end
  end
end
