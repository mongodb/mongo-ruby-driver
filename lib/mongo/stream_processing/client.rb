# frozen_string_literal: true

# Copyright (C) 2026-present MongoDB Inc.
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
  module StreamProcessing
    # Client for an Atlas Stream Processing workspace.
    #
    # Distinct from {Mongo::Client} so that connection intent is explicit and
    # ASP commands cannot be accidentally routed to a standard `mongod`. The
    # underlying {Mongo::Client} is still available via {#client} for advanced
    # uses such as `run_command` against admin.
    #
    # Workspace endpoints share the `mongodb://` URI scheme with standard
    # MongoDB clusters but follow a distinct hostname pattern:
    #
    #   mongodb://atlas-stream-<workspaceId>-<suffix>.<region>.a.query.mongodb.net/
    #
    # Atlas staging endpoints use `.a.query.mongodb-stage.net` instead; both
    # are accepted.
    #
    # Per the ASP spec, TLS is required and `authSource` defaults to `admin`.
    #
    # @since 2.25.0
    class Client
      # @return [ Mongo::Client ] The underlying client.
      attr_reader :client

      # @return [ String ] The workspace URI as given to the constructor.
      attr_reader :uri

      # @param uri [ String ] Workspace connection string.
      # @param options [ Hash ] Additional client options (passed through to
      #   {Mongo::Client}).
      def initialize(uri, options = {})
        unless self.class.workspace_uri?(uri)
          raise ArgumentError,
                'StreamProcessing::Client requires a workspace endpoint URI ' \
                '(atlas-stream-*.a.query.mongodb.net or .mongodb-<env>.net). ' \
                'For standard MongoDB clusters, use Mongo::Client instead.'
        end

        if uri.to_s.downcase.start_with?('mongodb+srv://')
          raise ArgumentError, 'mongodb+srv:// is not supported for workspace endpoints; use mongodb://'
        end

        options = options.dup
        # TLS is required and MUST NOT be disabled. The Ruby driver uses the
        # `:ssl` option (rather than `:tls`) to enable transport security.
        if options.key?(:ssl) && options[:ssl] == false
          raise ArgumentError, 'TLS cannot be disabled for an Atlas Stream Processing workspace connection'
        end

        options[:ssl] = true unless options.key?(:ssl)
        options[:auth_source] = 'admin' unless options.key?(:auth_source)

        @uri = uri
        @client = Mongo::Client.new(uri, options)
      end

      # Returns a handle for managing stream processors in this workspace.
      #
      # @return [ Processors ]
      def stream_processors
        Processors.new(@client)
      end

      # Closes the underlying client.
      def close
        @client.close
      end

      # Returns `true` when the supplied URI targets an Atlas Stream Processing
      # workspace endpoint.
      #
      # Matches hostnames that begin with `atlas-stream-` and end with
      # `.a.query.mongodb.net` (production) or `.a.query.mongodb-<env>.net`
      # (e.g. `mongodb-stage.net` for Atlas staging).
      #
      # @param uri [ String ]
      # @return [ Boolean ]
      def self.workspace_uri?(uri)
        return false unless uri.is_a?(String)

        lower = uri.downcase
        return false unless lower.start_with?('mongodb://')

        after_scheme = lower[10..]
        # Strip userinfo (if @ appears before the first /, ?).
        path_or_query = after_scheme.index(%r{[/?]}) || after_scheme.length
        at_idx = after_scheme.rindex('@', path_or_query - 1)
        host_section = at_idx ? after_scheme[(at_idx + 1)..] : after_scheme

        # Strip path/query/port.
        end_idx = host_section.index(%r{[/?:]}) || host_section.length
        host = host_section[0, end_idx]

        return false unless host.start_with?('atlas-stream-')
        return true if host.end_with?('.a.query.mongodb.net')

        # Accept .a.query.mongodb-<env>.net
        m = host.match(/\.a\.query\.mongodb-([a-z0-9-]+)\.net\z/)
        !m.nil?
      end
    end
  end
end
