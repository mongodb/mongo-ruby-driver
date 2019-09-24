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
RSpec::Matchers.define :have_blank_credentials do
  match do |client|
    %i(auth_mech auth_mech_properties auth_source password user).all? do |key|
      client.options[key].nil?
    end
  end

  failure_message do |client|
    "Expected client to have blank credentials, but got the following credentials: \n\n" +
      client.options.inspect
  end
end

module Mongo
  module Auth
    class Spec

      attr_reader :description
      attr_reader :tests

      def initialize(file)
        file = File.new(file)
        @spec = YAML.load(ERB.new(file.read).result)
        file.close
        @description = File.basename(file)
      end

      def tests
        @tests ||= @spec['tests'].collect do |spec|
          Test.new(spec)
        end
      end
    end

    class Test
      attr_reader :description
      attr_reader :uri_string

      def initialize(spec)
        @spec = spec
        @description = @spec['description']
        @uri_string = @spec['uri']
      end

      def valid?
        @spec['valid']
      end

      def credential
        @spec['credential']
      end

      def client
        @client ||= ClientRegistry.instance.new_local_client(@spec['uri'], monitoring_io: false)
      end

      def expected_credential
        expected_credential = { 'auth_source' => credential['source'] }

        if credential['username']
          expected_credential['user'] = credential['username']
          expected_credential['password'] = credential['password']
        end

        if credential['mechanism']
          expected_credential['auth_mech'] = expected_auth_mech
        end

        if credential['mechanism_properties']
          expected_credential['auth_mech_properties'] = expected_auth_mech_properties
        end

        expected_credential
      end

      def received_credential
        client.options.select do |k, _|
          %w(auth_mech auth_mech_properties auth_source password user).include?(k)
        end
      end

      private

      def expected_auth_mech
        Mongo::URI::AUTH_MECH_MAP[credential['mechanism']]
      end

      def expected_auth_mech_properties
        credential['mechanism_properties'].keys.map(&:downcase)
      end
    end
  end
end
