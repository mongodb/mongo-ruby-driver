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
RSpec::Matchers.define :match_credential do |test|
  def match_user?(client, credential)
    client.options[:user] == credential['username']
  end

  def match_password?(client, credential)
    client.options[:password] == credential['password']
  end

  def match_auth_source?(client, credential)
    expected_auth_source = credential['source'] == '$external' ? :external : credential['source']
    client.options[:auth_source] == expected_auth_source
  end

  def match_auth_mech?(client, credential)
    if credential['mechanism'].nil?
      expected_mechanism = nil
    else
      expected_mechanism = Mongo::URI::AUTH_MECH_MAP[credential['mechanism']]
    end

    client.options[:auth_mech] == expected_mechanism
  end

  def match_auth_mech_properties?(client, credential)
    if credential['mechanism_properties'].nil?
      return client.options[:auth_mech_properties].nil?
    end

    same_keys =
      client.options[:auth_mech_properties] == credential['mechanism_propertis'].keys.map(&:downcase)
    
    same_values =
      credential['mechanism_properties'].all? do |prop, prop_val|
        client.options[:auth_mech_properties][prop.downcase] == prop_val
      end

    same_keys && same_values
  end

  def blank_credentials?(client)
    %i(auth_mech auth_mech_properties auth_source password user).all? do |key|
      client.options[key].nil?
    end
  end

  match do |client|
    return blank_credentials?(client) if test.credential.nil?

    match_user?(client, test.credential) &&
      match_password?(client, test.credential) &&
      match_auth_source?(client, test.credential) &&
      match_auth_mech?(client, test.credential) &&
      match_auth_mech_properties?(client, test.credential)
  end

  failure_message do |client|
    "Expected that client initialized with URI #{test.uri_string} " +
      "would match credentials: \n\n#{test.credential} \n\n" +
      "but instead got: \n\n #{client.options}"
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
    end
  end
end
