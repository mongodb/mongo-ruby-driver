# frozen_string_literal: true

module Mongo
  class Collection
    class View
      class Aggregation
        # Distills the behavior common to aggregator classes, like
        # View::Aggregator and View::ChangeStream.
        module Behavior
          extend Forwardable
          include Enumerable
          include Immutable
          include Iterable
          include Explainable
          include Loggable
          include Retryable

          # @return [ View ] view The collection view.
          attr_reader :view

          # Delegate necessary operations to the view.
          def_delegators :view, :collection, :read, :cluster, :cursor_type, :limit, :batch_size

          # Delegate necessary operations to the collection.
          def_delegators :collection, :database, :client

          # Set to true if disk usage is allowed during the aggregation.
          #
          # @example Set disk usage flag.
          #   aggregation.allow_disk_use(true)
          #
          # @param [ true, false ] value The flag value.
          #
          # @return [ true, false, Aggregation ] The aggregation if a value was
          #   set or the value if used as a getter.
          #
          # @since 2.0.0
          def allow_disk_use(value = nil)
            configure(:allow_disk_use, value)
          end

          # Get the explain plan for the aggregation.
          #
          # @example Get the explain plan for the aggregation.
          #   aggregation.explain
          #
          # @return [ Hash ] The explain plan.
          #
          # @since 2.0.0
          def explain
            self.class.new(view, pipeline, options.merge(explain: true)).first
          end

          # Whether this aggregation will write its result to a database collection.
          #
          # @return [ Boolean ] Whether the aggregation will write its result
          #   to a collection.
          #
          # @api private
          def write?
            pipeline.any? { |op| op.key?('$out') || op.key?(:$out) || op.key?('$merge') || op.key?(:$merge) }
          end

          # @return [ Integer | nil ] the timeout_ms value that was passed as
          #   an option to this object, or which was inherited from the view.
          #
          # @api private
          def timeout_ms
            @timeout_ms || view.timeout_ms
          end

          private

          # Common setup for all classes that include this behavior; the
          # constructor should invoke this method.
          def perform_setup(view, options, forbid: [])
            @view = view

            @timeout_ms = options.delete(:timeout_ms)
            @options = BSON::Document.new(options).freeze

            yield

            validate_timeout_mode!(options, forbid: forbid)
          end

          def server_selector
            @view.send(:server_selector)
          end

          def aggregate_spec(session, read_preference)
            Builder::Aggregation.new(
              pipeline,
              view,
              options.merge(session: session, read_preference: read_preference)
            ).specification
          end

          # Skip, sort, limit, projection are specified as pipeline stages
          # rather than as options.
          def cache_options
            {
              namespace: collection.namespace,
              selector: pipeline,
              read_concern: view.read_concern,
              read_preference: view.read_preference,
              collation: options[:collation],
              # Aggregations can read documents from more than one collection,
              # so they will be cleared on every write operation.
              multi_collection: true,
            }
          end

          # @return [ Hash ] timeout_ms value set on the operation level (if any),
          #   and/or timeout_ms that is set on collection/database/client level (if any).
          #
          # @api private
          def operation_timeouts(opts = {})
            {}.tap do |result|
              if opts[:timeout_ms] || @timeout_ms
                result[:operation_timeout_ms] = opts.delete(:timeout_ms) || @timeout_ms
              else
                result[:inherited_timeout_ms] = view.timeout_ms
              end
            end
          end
        end
      end
    end
  end
end
