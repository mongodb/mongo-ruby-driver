# frozen_string_literal: true

module Tracing
  Error = Class.new(StandardError)

  class Span
    attr_reader :tracer, :name, :attributes, :with_parent, :kind, :finished, :nested

    attr_accessor :status

    def initialize(tracer, name, attributes = {}, with_parent: nil, kind: :internal)
      @tracer = tracer
      @name = name
      @attributes = attributes
      @with_parent = with_parent
      @kind = kind
      @finished = false
      @nested = []
    end

    def set_attribute(key, value)
      @attributes[key] = value
    end

    def record_exception(exception, attributes: nil)
      set_attribute('exception.type', exception.class.to_s)
      set_attribute('exception.message', exception.message)
      set_attribute(
        'exception.stacktrace',
        exception.full_message(highlight: false, order: :top).encode('UTF-8', invalid: :replace, undef: :replace,
                                                                              replace: '�')
      )
    end

    def finish
      raise Tracing::Error, 'Span already finished' if @finished

      @finished = true
      tracer.finish_span(self)
    end

    def inspect
      "#<Tracing::Span name=#{@name.inspect} attributes=#{@attributes.inspect}>"
    end
  end

  # Mock OpenTelemetry::Context to store and retrieve spans
  class Context
    attr_reader :span

    def initialize(span)
      @span = span
    end
  end

  class Tracer
    attr_reader :spans

    def initialize
      @spans = []
      @stack = []
      @active_context = nil
    end

    def start_span(name, attributes: {}, with_parent: nil, kind: :internal)
      parent = resolve_parent(with_parent)

      Span.new(self, name, attributes, with_parent: parent, kind: kind).tap do |span|
        @spans << span
        @stack << span
      end
    end

    def finish_span(span)
      raise Error, 'Span not found' unless @spans.include?(span)

      @stack.pop if @stack.last == span
    end

    def span_hierarchy
      # Build a mapping of all spans by their object_id for quick lookup
      span_map = {}
      @spans.each do |span|
        span_map[span.object_id] = span
      end

      # Build the hierarchy by attaching children to their parents
      root_spans = []
      @spans.each do |span|
        if span.with_parent.nil?
          # This is a root span
          root_spans << span
        else
          # Find the parent span and add this span to its nested array
          parent = span_map[span.with_parent.object_id]
          if parent
            parent.nested << span
          else
            raise Error, "Parent span not found for span #{span.name} (parent object_id: #{span.with_parent.object_id})"
          end
        end
      end

      root_spans
    end

    private

    # Resolve the parent span from various input types
    def resolve_parent(with_parent)
      return @stack.last if with_parent.nil?

      case with_parent
      when Tracing::Context
        # Extract span from our mock Context
        with_parent.span
      when Tracing::Span
        # Already a span
        with_parent
      when OpenTelemetry::Context
        # Extract span from OpenTelemetry::Context
        # The OpenTelemetry context stores the span using a specific key
        # We need to extract it using the OpenTelemetry::Trace API
        begin
          OpenTelemetry::Trace.current_span(with_parent)
        rescue
          # Fallback: try to extract from instance variables
          with_parent.instance_variable_get(:@entries)&.values&.first
        end
      else
        with_parent
      end
    end
  end
end
