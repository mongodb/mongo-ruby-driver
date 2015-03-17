# Copyright (C) 2009-2014 MongoDB, Inc.
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

  module MongoOrchestration

    class Spec

      attr_reader :description

      attr_reader :client

      attr_reader :mo

      attr_reader :base_url

      def initialize(file)
        @spec = YAML.load(ERB.new(File.new(file).read).result)
        @description = @spec['description']
        @client_setup = @spec['clientSetUp']
        @type = @spec['type']
        @init_config = @spec['initConfig']
        @base_url = @init_config['base_url']
      end

      def run
        setup do
          run_phases
          run_tests
        end
      end

      private

      def stop
        @mo.stop
      end

      def make_mo
        @mo ||= Resource.new(@type, @init_config)
      end


      def client_options
        @client_setup['options']
      end

      def hosts
        @hosts ||= @client_setup['hosts'].collect do |server_id|
                      m = @mo.hosts.find do |host|
                        host[:server_id] == server_id
                      end
                      m[:host]
                    end
      end

      def make_client
        @client ||= Mongo::Client.new(hosts, client_options)
      end

      def setup_resources
        make_mo
        make_client
      end

      def make_phases
        @phases ||= @spec['phases'].collect do |op|
          Operation.get(self, op)
        end
      end

      def make_tests
        @tests ||= @spec['tests'].collect do |test|
          Operation.get(self, test)
        end
      end

      def prepare_test
        make_phases
        make_tests
      end

      def run_phases
        phase = @phases.shift
        while phase && phase.run
          phase = @phases.shift
        end
      end

      def run_tests
        @tests.each do |test|
          test.run
        end
      end

      def setup
        begin
          setup_resources
          prepare_test
          yield
        rescue Errno::ECONNREFUSED
          raise ServiceNotAvailable.new
        rescue => ex
          stop
          raise ex
        end
        stop
      end
    end

    # Exception raised if the Mongo Orchestration service is
    # not available.
    #
    # @since 2.0.0
    class ServiceNotAvailable < Error

      # The error message.
      #
      # @since 2.0.0
      MESSAGE = 'The Mongo Orchestration service is not available'.freeze

      # Instantiate the new exception.
      #
      # @example Instantiate the exception.
      #   Mongo::Error::InvalidDocument.new
      #
      # @since 2.0.0
      def initialize
        super(MESSAGE)
      end
    end
  end
end
