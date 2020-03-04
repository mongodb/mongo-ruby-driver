module Mongo
  module CRUD
    class Requirement
      YAML_KEYS = %w(minServerVersion maxServerVersion topology).freeze

      def initialize(spec)
        @min_server_version = spec['minServerVersion']
        @max_server_version = spec['maxServerVersion']
        @topologies = if topologies = spec['topology']
          topologies.map do |topology|
            {'replicaset' => :replica_set, 'single' => :single, 'sharded' => :sharded}[topology]
          end
        else
          nil
        end
      end

      attr_reader :min_server_version
      attr_reader :max_server_version
      attr_reader :topologies

      def short_min_server_version
        if min_server_version
          min_server_version.split('.')[0..1].join('.')
        else
          nil
        end
      end

      def short_max_server_version
        if max_server_version
          max_server_version.split('.')[0..1].join('.')
        else
          nil
        end
      end

      def satisfied?
        cc = ClusterConfig.instance
        ok = true
        if short_min_server_version
          ok &&= cc.fcv_ish >= short_min_server_version
        end
        if max_server_version
          ok &&= cc.short_server_version <= max_server_version
        end
        if topologies
          ok &&= topologies.include?(cc.topology)
        end
        ok
      end

      def description
        versions = [min_server_version, max_server_version].compact
        if versions.any?
          versions = versions.join('-')
        else
          versions = nil
        end
        topologies = if self.topologies
          self.topologies.map(&:to_s).join(',')
        else
          nil
        end
        [versions, topologies].compact.join('/')
      end
    end
  end
end
