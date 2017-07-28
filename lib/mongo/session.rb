# Copyright (C) 2014-2016 MongoDB, Inc.
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

  class Session
    extend Forwardable

    attr_reader :client
    attr_reader :options

    def_delegators :client, :list_databases, :database_names

    AFTER_CLUSTER_TIME = 'afterClusterTime'.freeze

    def initialize(client, options = {})
      @client = client
      @options = client.options.merge(options)
      @server_session = ServerSession.new(@client)
      @ended = false
    end

    def end_session
      begin; @server_session.end_session; rescue; end
      @ended = true
    end

    def ended?
      @ended
    end

    def database(name)
      Database.new(client, name, client.options).tap do |db|
        db.instance_variable_set(:@session, self)
      end
    end

    def use
      begin
        yield
      ensure
        @server_session.update_last_use!
      end
    end

    def get_read_concern(collection)
      if causally_consistent_reads? && @operation_time
        collection.options[:read_concern].merge(AFTER_CLUSTER_TIME => @operation_time)
      else
        collection.options[:read_concern]
      end
    end

    private

    def causally_consistent_reads?
      options[:causally_consistent_reads]
    end

    def set_operation_time(result)
      @operation_time = result.operation_time
    end

    class ServerSession

      START_SESSION = { :startSession => 1 }.freeze

      SESSION_ID = 'id'.freeze

      TIMEOUT_MINUTES = 'timeoutMinutes'.freeze

      LAST_USE = 'lastUse'.freeze

      def initialize(client)
        start(client)
      end

      private

      def start(client)
        # with one retry
        server = ServerSelector.get(mode: :primary_preferred).select_server(client.cluster)
        response = Operation::Commands::Command.new({
                                                    :selector => START_SESSION,
                                                    :db_name => :admin,
                                                }).execute(server).first
        @session_id = response[SESSION_ID]['signedLsid']['lsid']
        @timeout_minutes = response[TIMEOUT_MINUTES]
        @last_use = response[SESSION_ID][LAST_USE]
      end

      def end_session(client)
        # with one retry
        Operation::Commands::Command.new({
                                             :selector => { :endSession => @uid },
                                             :db_name => :admin,
                                         }).execute(client.cluster.next_primary)
      end
    end
  end
end