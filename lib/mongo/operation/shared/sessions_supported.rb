# Copyright (C) 2015-2017 MongoDB, Inc.
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

      READ_PREFERENCE = '$readPreference'.freeze

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

      def apply_causal_consistency!(selector, server)
        return unless selector[:startTransaction]

        if !server.standalone?
          full_read_concern_doc = session.send(:causal_consistency_doc, selector[:readConcern] || read_concern)
          selector[:readConcern] = full_read_concern_doc if full_read_concern_doc
        end
      end

      def flags
        acknowledged_write? ? [:none] : [:more_to_come]
      end

      def apply_cluster_time!(selector, server)
        if !server.standalone?
          cluster_time = [server.cluster_time, (session && session.cluster_time)].max_by do |doc|
            (doc && doc[Cluster::CLUSTER_TIME]) || ZERO_TIMESTAMP
          end

          if cluster_time && (cluster_time[Cluster::CLUSTER_TIME] > ZERO_TIMESTAMP)
            selector[CLUSTER_TIME] = cluster_time
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

      def validate_read_pref!(selector)
        session.validate_read_pref!(selector) if read_command?(selector)
      end

      def update_session_state!
        session.update_state!
      end

      def command(server)
        sel = selector(server)
        add_write_concern!(sel)
        sel[Protocol::Msg::DATABASE_IDENTIFIER] = db_name
        sel[READ_PREFERENCE] = read.to_doc if read

        if server.features.sessions_enabled?
          apply_cluster_time!(sel, server)
          if session && (acknowledged_write? || session.in_transaction?)
            sel[:txnNumber] = BSON::Int64.new(txn_num) if txn_num
            apply_session_id!(sel)
            apply_start_transaction!(sel)
            apply_causal_consistency!(sel, server)
            apply_autocommit!(sel)
            apply_txn_opts!(sel)
            suppress_read_write_concern!(sel)
            validate_read_pref!(sel)
            update_session_state!
            apply_txn_num!(sel)
          end
        elsif session && session.explicit?
          apply_cluster_time!(sel, server)
          sel[:txnNumber] = BSON::Int64.new(txn_num) if txn_num
          apply_session_id!(sel)
          apply_start_transaction!(sel)
          apply_causal_consistency!(sel, server)
          apply_autocommit!(sel)
          apply_txn_opts!(sel)
          suppress_read_write_concern!(sel)
          validate_read_pref!(sel)
          update_session_state!
          apply_txn_num!(sel)
        end

        sel
      end
    end
  end
end
