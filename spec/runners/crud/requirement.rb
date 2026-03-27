# frozen_string_literal: true

module Mongo
  module CRUD
    class Requirement
      YAML_KEYS = %w[auth minServerVersion maxServerVersion topology topologies serverParameters csfle].freeze

      def initialize(spec)
        spec = spec.dup
        # Legacy tests have the requirements mixed with other test fields
        spec.delete('data')
        spec.delete('tests')

        unless (unhandled_keys = spec.keys - YAML_KEYS).empty?
          raise "Unhandled requirement specification keys: #{unhandled_keys}"
        end

        @min_server_version = spec['minServerVersion']
        @max_server_version = spec['maxServerVersion']
        # topologies is for unified test format.
        # topology is for legacy tests.
        @topologies = if topologies = spec['topology'] || spec['topologies']
                        topologies.map do |topology|
                          {
                            'replicaset' => :replica_set,
                            'single' => :single,
                            'sharded' => :sharded,
                            'sharded-replicaset' => :sharded,
                            'load-balanced' => :load_balanced,
                          }[topology].tap do |v|
                            raise "Unknown topology #{topology}" unless v
                          end
                        end
                      end
        @server_parameters = spec['serverParameters']
        @auth = spec['auth']
        @csfle = !!spec['csfle'] if spec['csfle']
      end

      attr_reader :min_server_version, :max_server_version, :topologies

      # `serverless` is a deprecated field. This module is a crutch to help
      # us through the transition period where some specs still have it.
      module DeprecatedServerless
        def self.prepended(base)
          new_list = [ *base::YAML_KEYS, 'serverless' ].freeze

          base.send(:remove_const, :YAML_KEYS) # rubocop:disable RSpec/RemoveConst
          base.send(:const_set, :YAML_KEYS, new_list)
        end

        attr_reader :serverless

        def initialize(spec)
          super
          initialize_serverless(spec)
        end

        private

        def initialize_serverless(spec)
          @serverless = if serverless = spec['serverless']
                          case spec['serverless']
                          when 'require' then :require
                          when 'forbid' then :forbid
                          when 'allow' then :allow
                          else raise "Unknown serverless requirement: #{serverless}"
                          end
                        end

          return unless @serverless && @serverless != :forbid

          warn 'The `serverless` requirement is deprecated.'
        end
      end

      prepend DeprecatedServerless

      def short_min_server_version
        return unless min_server_version

        min_server_version.split('.')[0..1].join('.')
      end

      def short_max_server_version
        return unless max_server_version

        max_server_version.split('.')[0..1].join('.')
      end

      def satisfied?
        cc = ClusterConfig.instance
        ok = true
        ok &&= Gem::Version.new(cc.fcv_ish) >= Gem::Version.new(min_server_version) if min_server_version
        ok &&= Gem::Version.new(cc.server_version) <= Gem::Version.new(max_server_version) if max_server_version
        ok &&= topologies.include?(cc.topology) if topologies
        if @server_parameters
          @server_parameters.each do |k, required_v|
            actual_v = cc.server_parameters[k]
            if actual_v.nil? && !required_v.nil?
              ok = false
            elsif actual_v != required_v
              if actual_v.is_a?(Numeric) && required_v.is_a?(Numeric)
                ok = false if actual_v.to_r != required_v.to_r
              else
                ok = false
              end
            end
          end
        end
        if @auth == true
          ok &&= SpecConfig.instance.auth?
        elsif @auth == false
          ok &&= !SpecConfig.instance.auth?
        end
        ok &&= !!(ENV['LIBMONGOCRYPT_PATH'] || ENV['FLE']) if @csfle
        ok
      end

      def description
        versions = [ min_server_version, max_server_version ].compact
        versions = (versions.join('-') if versions.any?)
        topologies = (self.topologies.map(&:to_s).join(',') if self.topologies)
        [ versions, topologies ].compact.join('/')
      end
    end
  end
end
