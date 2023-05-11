# frozen_string_literal: true
# rubocop:todo all

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

    # Read preference handling for pre-OP_MSG operation implementations.
    #
    # This module is not used by OP_MSG operation classes (those deriving
    # from OpMsgBase). Instead, read preference for those classes is handled
    # in SessionsSupported module.
    #
    # @since 2.5.2
    # @api private
    module ReadPreferenceSupported

      private

      # Get the options for executing the operation on a particular connection.
      #
      # @param [ Server::Connection ] connection The connection that the
      #   operation will be executed on.
      #
      # @return [ Hash ] The options.
      #
      # @since 2.0.0
      def options(connection)
        options = super
        if add_secondary_ok_flag?(connection)
          flags = options[:flags]&.dup || []
          flags << :secondary_ok
          options = options.merge(flags: flags)
        end
        options
      end

      # Whether to add the :secondary_ok flag to the request based on the
      # read preference specified in the operation or implied by the topology
      # that the connection's server is a part of.
      #
      # @param [ Server::Connection ] connection The connection that the
      #   operation will be executed on.
      #
      # @return [ true | false ] Whether the :secondary_ok flag should be added.
      def add_secondary_ok_flag?(connection)
        # https://github.com/mongodb/specifications/blob/master/source/server-selection/server-selection.rst#topology-type-single
        if connection.description.standalone?
          # Read preference is never sent to standalones.
          false
        elsif connection.server.cluster.single?
          # In Single topology the driver forces primaryPreferred read
          # preference mode (via the secondary_ok flag, in case of old servers)
          # so that the query is satisfied.
          true
        else
          # In replica sets and sharded clusters, read preference is passed
          # to the server if one is specified by the application, and there
          # is no default.
          read && read.secondary_ok? || false
        end
      end

      def command(connection)
        sel = super
        add_read_preference_legacy(sel, connection)
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
      # $readPreference is not sent to pre-OP_MSG replica set members.
      #
      # @param [ Hash ] sel Existing command document.
      # @param [ Server::Connection ] connection The connection that the
      #   operation will be executed on.
      #
      # @return [ Hash ] New command document to send to the server.
      def add_read_preference_legacy(sel, connection)
        if read && (
          connection.description.mongos? || connection.description.load_balancer?
        ) && read_pref = read.to_mongos
          # If the read preference contains only mode and mode is secondary
          # preferred and we are sending to a pre-OP_MSG server, this read
          # preference is indicated by the :secondary_ok wire protocol flag
          # and $readPreference command parameter isn't sent.
          if read_pref != {mode: 'secondaryPreferred'}
            Mongo::Lint.validate_camel_case_read_preference(read_pref)
            sel = sel[:$query] ? sel : {:$query => sel}
            sel = sel.merge(:$readPreference => read_pref)
          end
        end
        sel
      end
    end
  end
end
