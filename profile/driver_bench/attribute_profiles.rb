# frozen_string_literal: true

module Mongo
  module DriverBench
    # Reopens the command tracer so the benchmark can measure alternative
    # command-span attribute shapes end to end.
    #
    # Loaded only when OTEL_ATTRIBUTE_PROFILE names a profile, and only by the
    # benchmark. The driver never loads this file, and the tracer is untouched
    # for every configuration that does not set a profile.
    #
    # Profiles vary the command span only. The operation span carries a small,
    # fixed set of attributes, and the question this answers is about the
    # command span's attribute list.
    #
    # @api private
    module CommandAttributeProfiles
      # The profiles, and what each does to a command span.
      #
      #   none             zero attributes at creation, nothing deferred
      #   creation-only    the driver's creation-time set, nothing deferred
      #   all-at-creation  every attribute passed to start_span
      #   none-then-all    zero attributes at creation, all set afterwards
      #
      # A nil profile leaves the tracer alone; that is the driver as shipped.
      PROFILES = %w[ none creation-only all-at-creation none-then-all ].freeze

      class << self
        # @return [ String | nil ] the active profile.
        attr_reader :profile

        # Applies the profile named by OTEL_ATTRIBUTE_PROFILE, if any.
        def apply!
          name = ENV['OTEL_ATTRIBUTE_PROFILE']
          return if name.nil? || name.empty?

          raise ArgumentError, "unknown attribute profile #{name.inspect}" unless PROFILES.include?(name)

          @profile = name
          Mongo::Tracing::OpenTelemetry::CommandTracer.prepend(CommandTracerOverride)
        end
      end

      # Prepended into CommandTracer when a profile is active.
      module CommandTracerOverride
        # Builds the creation-time attributes for the active profile.
        def span_attributes(doc, name, connection)
          case CommandAttributeProfiles.profile
          when 'none', 'none-then-all'
            # Keep the connection so the deferred pass can build the
            # connection attributes; do not build anything now.
            Thread.current[:otel_bench_connection] = connection
            {}
          when 'all-at-creation'
            super.merge(deferred_attributes(name, doc))
          else
            super
          end
        end

        # Builds the deferred attributes for the active profile.
        def apply_deferred_attributes(span, message, name, doc, cursor)
          case CommandAttributeProfiles.profile
          when 'none', 'creation-only', 'all-at-creation'
            nil
          when 'none-then-all'
            connection = Thread.current[:otel_bench_connection]
            creation_attributes(name, doc, connection).each { |key, value| span.set_attribute(key, value) }
            deferred_attributes(name, doc, cursor).each { |key, value| span.set_attribute(key, value) }
          else
            super
          end
        end

        private

        # A copy of the driver's creation-time set, built independently so that
        # the none-then-all profile can defer it. Kept in step with
        # CommandTracer#span_attributes by hand; it is only used by the
        # benchmark.
        def creation_attributes(name, doc, connection)
          attrs = {
            'db.system.name' => 'mongodb',
            'db.namespace' => database(doc),
            'db.command.name' => name
          }
          if (coll_name = collection_name(name, doc))
            attrs['db.collection.name'] = coll_name
          end
          attrs.merge(connection_attributes(connection))
        end

        # A copy of the driver's deferred set. query_text is left out: it is
        # off by default and its value is built from the message, which
        # span_attributes does not receive.
        def deferred_attributes(name, doc, cursor = nil)
          cursor ||= cursor_id(name, doc)
          attrs = { 'db.query.summary' => query_summary(name, doc) }
          if (lsid_value = lsid(doc))
            attrs['db.mongodb.lsid'] = lsid_value
          end
          attrs['db.mongodb.cursor_id'] = cursor unless cursor.nil?
          if (txn = txn_number(doc))
            attrs['db.mongodb.txn_number'] = txn
          end
          attrs
        end
      end
    end
  end
end
