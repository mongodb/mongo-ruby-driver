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

require 'httparty'

module MongoOrchestration

  # Encapsulates behavior of a Mongo Orchestration resource.
  #
  # @since 2.0.0
  module Requestable
    include HTTParty

    # @return [ String ] base_path The path used to connect to the MO service.
    attr_reader :base_path
    alias_method :path, :base_path

    # Initialize a Mongo Orchestration resource.
    #
    # @example Initialize a Mongo Orchestration resource.
    #   MongoOrchestration::Standalone.new
    #
    # @param [ Hash ] options Options for creating the resource.
    #
    # @option options [ String ] :path The path to use for Mongo Orchestration.
    #
    # @since 2.0.0
    def initialize(options = {})
      @base_path = options[:path] || MongoOrchestration::DEFAULT_BASE_URI
      create(options)
    end

    # Is the Mongo Orchestration resource still available?
    #
    # @exmaple Check if the Mongo Orchestration resource is available.
    #   standalone.alive?
    #
    # @return [ true, false ] If the resource is available.
    #
    # @since 2.0.0
    def alive?
      begin
        get("servers/#{id}")
      rescue ServiceNotAvailable
        return false
      end
      !!(@config = @response if @response &&
                                  @response['procInfo'] &&
                                  @response['procInfo']['alive'])
    end

    private

    def http_request(method, path = nil, options = {})
      dispatch do
        abs_path = [@base_path, path].compact.join('/')
        options[:body] = options[:body].to_json if options.has_key?(:body)
        HTTParty.send(method, abs_path, options)
      end
    end

    def get(path = nil, options = {})
      http_request(__method__, path, options)
    end

    def post(path = nil, options = {})
      http_request(__method__, path, options)
    end

    def dispatch
      begin
        @response = yield
      rescue ArgumentError, Errno::ECONNREFUSED
        raise ServiceNotAvailable.new unless ok?
      end
    end

    def ok?
      @response && @response.code/100 == 2
    end
  end
end