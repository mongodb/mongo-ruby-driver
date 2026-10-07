# frozen_string_literal: true

require 'opentelemetry-sdk'
require 'mongo'

module Mongo
  module OtelAttributes
    # Measures the driver's own cost of producing each span attribute value:
    # the work that happens before the SDK sees an attribute and that the
    # driver can gate on the sampling decision.
    #
    # The span-shape sweep in SpanCost fixes the attribute values as literals
    # and measures the SDK's handling of them. This measures the other half:
    # extracting the value from the command document, the connection, or the
    # session. Together they add up to what an attribute costs end to end.
    #
    # No server is needed: the helpers are called directly with realistic
    # inputs. Caches (the lsid UUID, the per-connection attribute hash) are
    # warmed first, so the numbers are the steady-state cost the benchmark
    # tasks actually pay.
    #
    # Parameterised by the environment:
    #   ITERATIONS  calls per sample (default 200000)
    #   REPS        repetitions per measurement (default 5)
    #
    # @api private
    class ConstructionCost
      DEFAULT_ITERATIONS = 200_000
      DEFAULT_REPS = 5
      FORMAT = '%-44<attribute>s %12<ns>s %14<allocs>s'

      UUID = '6f1d0f2e-0f9a-4f3b-9a3e-2f2b1c4d5e6f'
      FIND_DOC = {
        'find' => 'corpus',
        'filter' => { '_id' => 1 },
        'lsid' => { 'id' => BSON::Binary.new([ UUID.delete('-') ].pack('H*'), :uuid) },
        'txnNumber' => BSON::Int64.new(3),
        '$db' => 'perftest'
      }.freeze
      GET_MORE_DOC = {
        'getMore' => BSON::Int64.new(123_456_789),
        'collection' => 'corpus',
        '$db' => 'perftest'
      }.freeze

      # A stand-in for Mongo::Server::Connection that answers the methods
      # connection_attributes reads.
      class FakeConnection
        Address = Struct.new(:host, :port)
        Description = Struct.new(:server_connection_id)

        def initialize
          @address = Address.new('localhost', 27_017)
          @description = Description.new(42)
        end

        attr_reader :address, :description

        def transport
          :tcp
        end

        def id
          7
        end
      end

      # A stand-in for a Mongo::Operation that answers what the operation
      # tracer reads.
      class FakeOperation
        attr_reader :db_name, :coll_name

        def initialize
          @db_name = 'perftest'
          @coll_name = 'corpus'
        end
      end

      def self.run!
        new.run
      end

      def initialize(env = ENV)
        @iterations = Integer(env['ITERATIONS'] || DEFAULT_ITERATIONS)
        @reps = Integer(env['REPS'] || DEFAULT_REPS)
        @otel_tracer = ::OpenTelemetry::SDK::Trace::TracerProvider
                       .new(sampler: ::OpenTelemetry::SDK::Trace::Samplers::ALWAYS_ON)
                       .tracer('otel-attribute-cost', '1.0')
        @command_tracer = Mongo::Tracing::OpenTelemetry::CommandTracer.new(@otel_tracer, nil)
        @operation_tracer = Mongo::Tracing::OpenTelemetry::OperationTracer.new(@otel_tracer, nil)
        @connection = FakeConnection.new
        @operation = FakeOperation.new
      end

      # Runs every measurement and returns the table.
      #
      # @return [ String ] the summary table.
      def run
        prepare
        lines = [ header, column_header ]
        measurements.each do |label, block|
          ns, allocs = sample(&block)
          lines << format(FORMAT, attribute: label, ns: format('%.1f', ns), allocs: format('%.2f', allocs))
        end
        lines.join("\n")
      end

      private

      # Warms every cache the helpers use, so the measurements are steady
      # state rather than first-call cost.
      def prepare
        @command_tracer.send(:connection_attributes, @connection)
        @command_tracer.send(:lsid, FIND_DOC)
        @span = @otel_tracer.start_span('warmup', kind: :client)
      end

      def header
        format('===== Driver cost of building one command span attribute (%d calls, median of %d reps) =====',
               @iterations, @reps)
      end

      def column_header
        format(FORMAT, attribute: 'attribute', ns: 'ns/call', allocs: 'allocs/call')
      end

      # Each entry is a label and a callable. The callables take the iteration
      # index that Integer#times yields and ignore it.
      def measurements
        [
          [ 'database(doc)', ->(_i) { @command_tracer.send(:database, FIND_DOC) } ],
          [ 'command_name(doc)', ->(_i) { @command_tracer.send(:command_name, FIND_DOC) } ],
          [ 'collection_name(name, doc)', ->(_i) { @command_tracer.send(:collection_name, 'find', FIND_DOC) } ],
          [ 'query_summary(name, doc)', ->(_i) { @command_tracer.send(:query_summary, 'find', FIND_DOC) } ],
          [ 'txn_number(doc)', ->(_i) { @command_tracer.send(:txn_number, FIND_DOC) } ],
          [ 'cursor_id(name, doc)', ->(_i) { @command_tracer.send(:cursor_id, 'getMore', GET_MORE_DOC) } ],
          [ 'lsid(doc) (warm cache)', ->(_i) { @command_tracer.send(:lsid, FIND_DOC) } ],
          [ 'connection_attributes(conn) (warm)',
            ->(_i) { @command_tracer.send(:connection_attributes, @connection) } ],
          [ 'span_attributes(doc, name, conn)',
            ->(_i) { @command_tracer.send(:span_attributes, FIND_DOC, 'find', @connection) } ],
          [ 'apply_deferred_attributes(recording span)',
            ->(_i) { @command_tracer.send(:apply_deferred_attributes, @span, nil, 'find', FIND_DOC, 123_456_789) } ],
          [ 'operation span_attributes (creation set)',
            ->(_i) { @operation_tracer.send(:span_attributes, @operation, 'find', 'find perftest.corpus', 'corpus') } ],
          [ 'operation collection_name(op)', ->(_i) { @operation_tracer.send(:collection_name, @operation) } ]
        ]
      end

      # Times a block +@reps+ times and returns the median per-call CPU
      # nanoseconds excluding GC, and allocations per call.
      #
      # @return [ Array(Float, Float) ] [ ns/call, allocs/call ].
      def sample(&block)
        runs = Array.new(@reps) { time(&block) }
        [ median(runs.map(&:first)), median(runs.map(&:last)) ]
      end

      def time(&block)
        GC.start
        allocs_before = GC.stat(:total_allocated_objects)
        gc_before = GC.stat(:time)
        cpu_before = Process.clock_gettime(Process::CLOCK_PROCESS_CPUTIME_ID)
        @iterations.times(&block)
        cpu_ns = (Process.clock_gettime(Process::CLOCK_PROCESS_CPUTIME_ID) - cpu_before) * 1e9 / @iterations
        gc_ns = (GC.stat(:time) - gc_before) * 1_000_000.0 / @iterations
        [ cpu_ns - gc_ns, (GC.stat(:total_allocated_objects) - allocs_before).to_f / @iterations ]
      end

      def median(values)
        sorted = values.sort
        mid = sorted.length / 2
        sorted.length.odd? ? sorted[mid] : (sorted[mid - 1] + sorted[mid]) / 2.0
      end
    end
  end
end

puts Mongo::OtelAttributes::ConstructionCost.run! if $PROGRAM_NAME == __FILE__
