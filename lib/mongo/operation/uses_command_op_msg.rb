# Copyright (C) 2017 MongoDB, Inc.
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

    # A command that uses OP_MSG, with the document as payload type 0.
    #
    # @since 2.5.0
    module UsesCommandOpMsg

      private

      ZERO_TIMESTAMP = BSON::Timestamp.new(0,0)

      READ_PREFERENCE = '$readPreference'.freeze

      def apply_causal_consistency!(selector, server); end

      def apply_cluster_time!(selector, server)
        if !server.standalone?
          cluster_time = [ server.cluster_time, (session && session.cluster_time) ].max_by do |doc|
                            (doc && doc[Cluster::CLUSTER_TIME]) || ZERO_TIMESTAMP
                          end

          if cluster_time && (cluster_time[Cluster::CLUSTER_TIME] > ZERO_TIMESTAMP)
            selector[CLUSTER_TIME] = cluster_time
          end
        end
      end

      def apply_session_id!(selector)
        session.add_id!(selector) if session && !unacknowledged_write?
      end

      def unacknowledged_write?
        write_concern && write_concern.get_last_error.nil?
      end

      def update_selector_for_session!(selector, server)
        # the driver MUST ignore any implicit session if at the point it is sending a command
        # to a specific server it turns out that that particular server doesn't support sessions after all
        if server.features.sessions_enabled?
          apply_cluster_time!(selector, server)
          selector[:txnNumber] = BSON::Int64.new(txn_num) if txn_num
          if session
            apply_session_id!(selector)
            apply_causal_consistency!(selector, server)
          end
        elsif session && !session.implicit?
          apply_cluster_time!(selector, server)
          apply_session_id!(selector)
          apply_causal_consistency!(selector, server)
          selector[:txnNumber] = BSON::Int64.new(txn_num) if txn_num
        end
      end

      def command_op_msg(server, selector, options)
        update_selector_for_session!(selector, server)
        selector[Protocol::Msg::DATABASE_IDENTIFIER] = db_name
        selector[READ_PREFERENCE] = read.to_doc if read
        flags = unacknowledged_write? ? [:more_to_come] : [:none]
        Protocol::Msg.new(flags, options, selector)
      end
    end
  end
end
