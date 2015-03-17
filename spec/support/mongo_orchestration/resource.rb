# Copyright (C) 2014-2015 MongoDB, Inc.
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

require 'httparty'

module Mongo

  module MongoOrchestration

    class Resource

      attr_reader :hosts

      def initialize(type, config)
        @type = type.freeze
        @config = config.freeze
        @base_url = config['base_url'].freeze || DEFAULT_URL
        setup
      end

      def stop
        delete(id)
      end

      def available?
        get
      end

      def request(method, uri, payload = nil)
        path = @base_url + uri
        options = payload ?  { :body => payload.to_json } : {}
        HTTParty.send(method.downcase.to_sym, path, options)
      end

      private

      def id
        @info['id']
      end

      def setup
        @info = post(@config)
        @hosts = @info['members'].collect do |member|
          { server_id: member['server_id'],
            host: member['host']
          }
        end
      end

      def post(payload, path = nil)
        url = path ? [resource_url, path].join('/') : resource_url
        HTTParty.post(url, :body => payload.to_json)
      end

      def get(path = nil)
        url = path ? [resource_url, path].join('/') : resource_url
        HTTParty.get(url)
      end

      def delete(path = nil)
        url = path ? [resource_url, path].join('/') : resource_url
        HTTParty.delete(url)
      end

      def resource_url
        [@base_url, @type].join('/')
      end
    end
  end
end
