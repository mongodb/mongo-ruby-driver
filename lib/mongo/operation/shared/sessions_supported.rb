# Copyright (C) 2015-2019 MongoDB, Inc.
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
      def apply_causal_consistency!(selector, server)
        return unless selector[:startTransaction]

        apply_causal_consistency_if_possible(selector, server)
      end

      # Adds causal consistency document to the selector, if one can be
      # constructed.
      #
      # In order for the causal consistency document to be constructed,
      # causal consistency must be enabled for the session and the session
      # must have the current operation time. Also, topology must be
      # replica set or sharded cluster.
      def apply_causal_consistency_if_possible(selector, server)
        if !server.standalone?
          cc_doc = session.send(:causal_consistency_doc)
          if cc_doc
            selector[:readConcern] = (selector[:readConcern] || read_concern || {}).merge(cc_doc)
          end
        end
      end

      def flags
        acknowledged_write? ? [] : [:more_to_come]
      end

      def apply_cluster_time!(selector, server)
        if !server.standalone?
          cluster_time = [server.cluster_time, session && session.cluster_time].compact.max

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

      def apply_session_id!(selector)
        session.add_id!(selector)
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

      def command(server)
        sel = selector(server).dup
        add_write_concern!(sel)
        sel[Protocol::Msg::DATABASE_IDENTIFIER] = db_name
        sel['$readPreference'] = read.to_doc if read

        if server.features.sessions_enabled?
          apply_cluster_time!(sel, server)
          if session && (acknowledged_write? || session.in_transaction?)
            apply_session_options(sel, server)
          end
        elsif session && session.explicit?
          apply_session_options(sel, server)
        end

        sel
      end

      def apply_session_options(sel, server)
        apply_cluster_time!(sel, server)
        sel[:txnNumber] = BSON::Int64.new(txn_num) if txn_num
        apply_session_id!(sel)
        apply_start_transaction!(sel)
        apply_causal_consistency!(sel, server)
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

      def build_message(server)
        super.tap do |message|
          if session
            # Serialize the message to detect client-side problems,
            # such as invalid BSON keys. The message will be serialized again
            # later prior to being sent to the server.
            message.serialize(BSON::ByteBuffer.new)

            session.update_state!
          end
        end
      end
    end
  end
end
