# Copyright (C) 2015-2020 MongoDB Inc.
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
  module Operation

    # Shared behavior of operations that support a session.
    #
    # @since 2.5.2
    module SessionsSupported

      private

      ZERO_TIMESTAMP = BSON::Timestamp.new(0, 0)

      READ_COMMANDS = [
        :aggregate,
        :collStats,
        :count,
        :dbStats,
        :distinct,
        :find,
        :geoNear,
        :geoSearch,
        :group,
        :mapReduce,
        :parallelCollectionScan
      ].freeze

      # Adds causal consistency document to the selector, if one can be
      # constructed and the selector is for a startTransaction command.
      #
      # When operations are performed in a transaction, only the first
      # operation (the one which starts the transaction via startTransaction)
      # is allowed to have a read concern, and with it the causal consistency
      # document, specified.
      def apply_causal_consistency!(selector, connection)
        return unless selector[:startTransaction]

        apply_causal_consistency_if_possible(selector, connection)
      end

      # Adds causal consistency document to the selector, if one can be
      # constructed.
      #
      # In order for the causal consistency document to be constructed,
      # causal consistency must be enabled for the session and the session
      # must have the current operation time. Also, topology must be
      # replica set or sharded cluster.
      def apply_causal_consistency_if_possible(selector, connection)
        if !connection.standalone?
          cc_doc = session.send(:causal_consistency_doc)
          if cc_doc
            rc_doc = (selector[:readConcern] || read_concern || {}).merge(cc_doc)
            selector[:readConcern] = Options::Mapper.transform_values_to_strings(
              rc_doc)
          end
        end
      end

      def flags
        acknowledged_write? ? [] : [:more_to_come]
      end

      def apply_cluster_time!(selector, connection)
        if !connection.standalone?
          cluster_time = [connection.cluster_time, session && session.cluster_time].compact.max

          if cluster_time
            selector['$clusterTime'] = cluster_time
          end
        end
      end

      def read_command?(sel)
        READ_COMMANDS.any? { |c| sel[c] }
      end

      def add_write_concern!(sel)
        sel[:writeConcern] = write_concern.options if write_concern
      end

      def apply_autocommit!(selector)
        session.add_autocommit!(selector)
      end

      def apply_start_transaction!(selector)
        session.add_start_transaction!(selector)
      end

      def apply_txn_num!(selector)
        session.add_txn_num!(selector)
      end

      def apply_read_pref!(selector)
        session.apply_read_pref!(selector) if read_command?(selector)
      end

      def apply_txn_opts!(selector)
        session.add_txn_opts!(selector, read_command?(selector))
      end

      def suppress_read_write_concern!(selector)
        session.suppress_read_write_concern!(selector)
      end

      def validate_read_preference!(selector)
        session.validate_read_preference!(selector) if read_command?(selector)
      end

      def command(connection)
        if Lint.enabled?
          unless connection.is_a?(Server::Connection)
            raise Error::LintError, "Connection is not a Connection instance: #{connection}"
          end
        end

        sel = selector(connection).dup
        add_write_concern!(sel)
        sel[Protocol::Msg::DATABASE_IDENTIFIER] = db_name

        add_read_preference(sel, connection)

        if connection.features.sessions_enabled?
          apply_cluster_time!(sel, connection)
          if session && (acknowledged_write? || session.in_transaction?)
            apply_session_options(sel, connection)
          end
        elsif session && session.explicit?
          apply_session_options(sel, connection)
        end

        sel
      end

      # Adds $readPreference field to the command document.
      #
      # $readPreference is only sent when the server is a mongos,
      # following the rules described in
      # https://github.com/mongodb/specifications/blob/master/source/server-selection/server-selection.rst#passing-read-preference-to-mongos.
      # The topology does not matter for figuring out whether to send
      # $readPreference since the decision is always made based on
      # server type.
      #
      # $readPreference is sent to OP_MSG-grokking replica set members.
      #
      # @param [ Hash ] sel Existing command document which will be mutated.
      # @param [ Server::Connection ] connection The connection that the
      #   operation will be executed on.
      def add_read_preference(sel, connection)
        if Lint.enabled?
          unless connection.is_a?(Server::Connection)
            raise Error::LintError, "Connection is not a Connection instance: #{connection}"
          end
        end

        # https://github.com/mongodb/specifications/blob/master/source/server-selection/server-selection.rst#topology-type-single
        if connection.server.standalone?
          # Read preference is never sent to standalones.
        elsif connection.server.cluster.single?
          # In Single topology:
          # - If no read preference is specified by the application, the driver
          #   adds mode: primaryPreferred.
          # - If a read preference is specified by the application, the driver
          #   replaces the mode with primaryPreferred.
          read_doc = if read
            BSON::Document.new(read.to_doc)
          else
            BSON::Document.new
          end
          if [nil, 'primary'].include?(read_doc['mode'])
            read_doc['mode'] = 'primaryPreferred'
          end
          sel['$readPreference'] = read_doc
        else
          # In replica sets and sharded clusters, read preference is passed
          # to the server if one is specified by the application, and there
          # is no default.
          if read
            sel['$readPreference'] = read.to_doc
          end
        end
      end

      def apply_session_options(sel, connection)
        apply_cluster_time!(sel, connection)
        sel[:txnNumber] = BSON::Int64.new(txn_num) if txn_num
        sel.merge!(lsid: session.session_id)
        apply_start_transaction!(sel)
        apply_causal_consistency!(sel, connection)
        apply_autocommit!(sel)
        apply_txn_opts!(sel)
        suppress_read_write_concern!(sel)
        validate_read_preference!(sel)
        apply_txn_num!(sel)
        if session.recovery_token &&
          (sel[:commitTransaction] || sel[:abortTransaction])
        then
          sel[:recoveryToken] = session.recovery_token
        end
      end

      def build_message(connection)
        super.tap do |message|
          if session
            # Serialize the message to detect client-side problems,
            # such as invalid BSON keys. The message will be serialized again
            # later prior to being sent to the connection.
            message.serialize(BSON::ByteBuffer.new)

            session.update_state!
          end
        end
      end
    end
  end
end
