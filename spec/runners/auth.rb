# frozen_string_literal: true
# rubocop:todo all

# Copyright (C) 2014-2020 MongoDB Inc.
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
    # The "null credential" definition in auth spec tests readme at
    # https://github.com/mongodb/specifications/blob/master/source/auth/tests/README.rst
    # is as follows:
    #
    # credential: If null, the credential must not be considered configured
    # for the the purpose of deciding if the driver should authenticate to the
    # topology.
    #
    # Ruby driver authenticates if :user or :auth_mech client options are set.
    #
    # Note that this is a different test from "no auth-related options are
    # set on the client". Options like password or auth source are preserved
    # by the client if set, but do not trigger authentication.
    %i(auth_mech user).all? do |key|
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

      def initialize(test_path)
        @spec = ::Utils.load_spec_yaml_file(test_path)
        @description = File.basename(test_path)
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
        end

        if credential['password']
          expected_credential['password'] = credential['password']
        end

        if credential['mechanism']
          expected_credential['auth_mech'] = expected_auth_mech
        end

        if credential['mechanism_properties']
          props = Hash[credential['mechanism_properties'].map do |k, v|
            [k.downcase, v]
          end]
          expected_credential['auth_mech_properties'] = props
        end

        expected_credential
      end

      def actual_client_options
        client.options.select do |k, _|
          %w(auth_mech auth_mech_properties auth_source password user).include?(k)
        end
      end

      def actual_user_attributes
        user = Mongo::Auth::User.new(client.options)
        attrs = {}
        {
          auth_mech_properties: 'auth_mech_properties',
          auth_source: 'auth_source',
          name: 'user',
          password: 'password',
          mechanism: 'auth_mech',
        }.each do |attr, field|
          value = user.send(attr)
          unless value.nil? || attr == :auth_mech_properties && value == {}
            attrs[field] = value
          end
        end
        attrs
      end

      private

      def expected_auth_mech
        Mongo::URI::AUTH_MECH_MAP[credential['mechanism']]
      end
    end
  end
end
