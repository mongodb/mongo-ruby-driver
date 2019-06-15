# Copyright (C) 2014-2019 MongoDB, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

module Mongo
  module Transactions
    class Operation < Mongo::CRUD::Operation
      include RSpec::Matchers

      def execute(collection, session0, session1, active_session=nil)
        # Determine which object the operation method should be called on.
        obj = case object
        when 'session0'
          session0
        when 'session1'
          session1
        when 'database'
          collection.database
        else
          if rp = collection_read_preference
            collection = collection.with(read: rp)
          end
          collection = collection.with(read_concern: read_concern) if read_concern
          collection = collection.with(write: write_concern) if write_concern
          collection
        end

        session = case arguments && arguments['session']
        when 'session0'
          session0
        when 'session1'
          session1
        else
          # active session could be nil
          active_session
        end

        context = Context.new(
          session0,
          session1,
          session)

        op_name = Utils.underscore(name).to_sym
        if op_name == :with_transaction
          args = [collection]
        else
          args = []
        end
        if op_name.nil?
          raise "Unknown operation #{name}"
        end
        result = send(op_name, obj, context, *args)
        if result
          if result.is_a?(Hash)
            result = result.dup
            result['error'] = false
          end
        end
        result
      rescue Mongo::Error::OperationFailure => e
        err_doc = e.instance_variable_get(:@result).send(:first_document)
        error_code_name = err_doc['codeName'] || err_doc['writeConcernError'] && err_doc['writeConcernError']['codeName']
        if error_code_name.nil?
          # Sometimes the server does not return the error code name,
          # but does return the error code (or we can parse the error code
          # out of the message).
          # https://jira.mongodb.org/browse/SERVER-39706
          warn "Error without error code name: #{e.code}"
        end

        {
          'errorCode' => e.code,
          'errorCodeName' => e.code_name,
          'errorContains' => e.message,
          'errorLabels' => e.labels,
          'exception' => e,
          'error' => true,
        }
      rescue Mongo::Error => e
        {
          'errorContains' => e.message,
          'errorLabels' => e.labels,
          'exception' => e,
          'error' => true,
        }
      # We do not have a base class for client side BSON-related errors.
      # See https://jira.mongodb.org/browse/RUBY-1806.
      # Rescue this particular exception for the time being.
      rescue BSON::String::IllegalKey => e
        {
          'exception' => e,
          'clientError' => true,
          'error' => true,
        }
      end

      private

      # operations

      def run_command(database, context)
        # Convert the first key (i.e. the command name) to a symbol.
        cmd = arguments['command'].dup
        command_name = cmd.first.first
        command_value = cmd.delete(command_name)
        cmd = { command_name.to_sym => command_value }.merge(cmd)

        opts = Utils.snakeize_hash(context.transform_arguments(options)).dup
        opts[:read] = opts.delete(:read_preference)
        database.command(cmd, opts).documents.first
      end

      def start_transaction(session, context)
        session.start_transaction(Utils.snakeize_hash(arguments['options'])) ; nil
      end

      def commit_transaction(session, context)
        session.commit_transaction ; nil
      end

      def abort_transaction(session, context)
        session.abort_transaction ; nil
      end

      def with_transaction(session, context, collection)
        unless callback = arguments['callback']
          raise ArgumentError, 'with_transaction requires a callback to be present'
        end

        if arguments['options']
          options = Utils.snakeize_hash(arguments['options'])
        else
          options = nil
        end
        session.with_transaction(options) do
          callback['operations'].each do |op_spec|
            op = Operation.new(op_spec)
            rv = op.execute(collection, context.session0, context.session1, session)
            if rv && rv['exception']
              raise rv['exception']
            end
          end
        end
      end

      def assert_session_transaction_state(collection, context)
        session = context.send(arguments['session'])
        actual_state = session.instance_variable_get('@state').to_s.sub(/^transaction_|_transaction$/, '').sub(/^no$/, 'none')
        expect(actual_state).to eq(arguments['state'])
      end

      def targeted_fail_point(collection, context)
        args = context.transform_arguments(options)
        session = args[:session]
        unless session.pinned_server
          raise ArgumentError, 'Targeted fail point requires session to be pinned to a server'
        end

        client = ClusterTools.instance.direct_client(session.pinned_server.address,
          database: 'admin')
        client.command(arguments['failPoint'])

        $disable_fail_points ||= []
        $disable_fail_points << [
          arguments['failPoint'],
          session.pinned_server.address,
        ]
      end

      def assert_session_pinned(collection, context)
        args = context.transform_arguments(options)
        session = args[:session]
        unless session.pinned_server
          raise ArgumentError, 'Expected session to be pinned'
        end
      end

      def assert_session_unpinned(collection, context)
        args = context.transform_arguments(options)
        session = args[:session]
        if session.pinned_server
          raise ArgumentError, 'Expected session to not be pinned'
        end
      end

      def read_concern
        Utils.snakeize_hash(@spec['collectionOptions'] && @spec['collectionOptions']['readConcern'])
      end

      def write_concern
        Utils.snakeize_hash(@spec['collectionOptions'] && @spec['collectionOptions']['writeConcern'])
      end

      def collection_read_preference
        Utils.snakeize_hash(@spec['collectionOptions'] && @spec['collectionOptions']['readPreference'])
      end
    end
  end
end
