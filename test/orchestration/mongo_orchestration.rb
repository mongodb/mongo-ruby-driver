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

require 'mongo'
require 'httparty'
require 'pp'

module Mongo
  module Orchestration

    class Base
      include HTTParty

      DEFAULT_BASE_URI = 'http://localhost:8889'
      base_uri (ENV['MONGO_ORCHESTRATION'] || DEFAULT_BASE_URI)
      attr_reader :base_path, :method, :abs_path, :response

      @@debug = false

      def debug
        @@debug
      end

      def debug=(value)
        @@debug = value
      end

      def initialize(base_path = '')
        @base_path = base_path
      end

      def http_request(method, path = nil, options = {})
        @method = method
        @abs_path = [@base_path, path].compact.join('/')
        @options = options
        @options[:body] = @options[:body].to_json if @options.has_key?(:body)
        @response = self.class.send(@method, @abs_path, @options)
        puts message_summary if debug
        self
      end

      def post(path = nil, options)
        http_request(__method__, path, options)
      end

      def get(path = nil, options = {})
        http_request(__method__, path, options)
      end

      def put(path = nil, options = {})
        http_request(__method__, path, options)
      end

      def delete(path = nil, options = {})
        http_request(__method__, path, options)
      end

      def ok
        (@response.code/100) == 2
      end

      def humanized_http_response_class_name
        @response.response.class.name.split('::').last.sub(/^HTTP/, '').gsub(/([a-z\d])([A-Z])/, '\1 \2')
      end

      def message_summary
        msg = "#{@method.upcase} #{@abs_path}, options: #{@options.inspect}"
        msg += ", #{@response.code} #{humanized_http_response_class_name}"
        return msg if @response.headers['content-length'] == "0" # not Fixnum 0
        if @response.headers['content-type'].include?('application/json')
          begin
            msg += ", response JSON:\n#{JSON.pretty_generate(@response)}"
          rescue Exception => ex
            #puts "msg:#{msg.inspect} @response.body: #{@response.body.inspect}"
            msg += ", response Ruby:\n#{@response.inspect}"
          end
        else
          msg += ", response: #{@response.inspect}"
        end
      end
    end

    class Resource < Base
      attr_reader :request_content, :object

      def initialize(base_path = '', request_content = nil)
        super(base_path)
        @request_content = request_content
        get
      end

      def get(path = nil, options = {})
        super
        @object = @response.parsed_response if ok
        self
      end

      def sub_resource(sub_class, path)
        abs_path = [@base_path, path].join('/')
        sub_rsrc = sub_class.new(abs_path)
        raise "Sub-resource error #{message_summary}" unless sub_rsrc.ok
        sub_rsrc
      end
    end

    class Service < Resource
      VERSION_REQUIRED = "0.9"

      def initialize(base_path = '')
        super
        check_service
      end

      def check_service
        get
        raise "mongo-orchestration service #{base_uri.inspect} is not available. Please start it via 'python server.py start'" if @response.code == 404
        version = @response.parsed_response['version']
        raise "mongo-orchestration service version #{version.inspect} is insufficient, #{VERSION_REQUIRED} is required" if version < VERSION_REQUIRED
        self
      end
    end

    class Topology < Resource
      def status
        get
      end

      def create
        put(nil, {body: @request_content})
        if ok
          @object = @response.parsed_response
        else
          raise "#{self.class.name}##{__method__} #{message_summary}"
        end
        self
      end

      def init
        create unless status.ok
        self
      end

      def destroy
        delete
        raise "#{self.class.name}##{__method__} #{message_summary}" unless [204, 404].include?(@response.code)
        self
      end

      def reset
        post(nil, {body: {action: __method__}})
      end

      private
      def sub_resource_servers(get_resource)
        sub_rsrc = sub_resource(Resource, get_resource)
        [sub_rsrc.object].flatten(1).collect{|object| Server.new(object['uri'])}
      end

      def component(klass, path, object, id_key)
        base_path = ((path =~ %r{^/}) ? '' : "#{@base_path}/") + "#{path}/#{object[id_key]}"
        klass.new(base_path)
      end

      def components(get_resource, klass, resource, id_key)
        sub_rsrc = sub_resource(Resource, get_resource)
        [sub_rsrc.object].flatten(1).collect{|host_data| component(klass, resource, host_data, id_key)}
      end
    end

    class Server < Topology
      %w[start stop restart freeze].each do |name|
        define_method(name) do
          post(nil, {body: {action: name}})
        end
      end

      def stepdown # TODO - mongo orchestration {replSetStepDown: 60, force: true}, then simplify this
        begin
          post(nil, {body: {action: __method__}})
        end until ok
        self
      end
    end

    class ReplicaSet < Topology
      def member_resources
        components('members', Resource, 'members', 'member_id')
      end

      def primary
        sub_resource_servers(__method__).first
      end

      %w[members servers secondaries arbiters hidden].each do |name|
        define_method(name) do
          sub_resource_servers(name)
        end
      end
    end

    class ShardedCluster < Topology
      def shard_resources
        components('shards', Resource, 'shards', 'shard_id')
      end

      def shards
        resource = sub_resource(Resource, 'shards')
        resource.ok ? resource.object.collect{|member| shard(member)} : []
      end

      %w[configsvrs routers].each do |name|
        define_method(name) do
          sub_resource_servers(name)
        end
      end

      private
      def shard(object)
        return ReplicaSet.new(object['uri']) if object.has_key?('isReplicaSet')
        return Server.new(object['uri']) if object.has_key?('isServer')
        nil
      end
    end

    class Service
      ORCHESTRATION_CLASS = { 'servers' => Server, 'replica_sets' => ReplicaSet, 'sharded_clusters' => ShardedCluster }

      def configure(config)
        orchestration = config[:orchestration]
        request_content = config[:request_content]
        klass = ORCHESTRATION_CLASS[orchestration]
        id = request_content[:id]
        unless id
          http_request = Base.new.post(orchestration, {:body => request_content})
          id = http_request.response.parsed_response['id']
        end
        base_path = [@base_path, orchestration, id].join('/')
        topology = klass.new(base_path, request_content)
        topology.init
      end
    end
  end
end
