# frozen_string_literal: true

module Tracing
  Error = Class.new(StandardError)

  class Span
    attr_reader :tracer, :name, :attributes, :events, :with_parent, :kind, :finished, :nested

    attr_accessor :status

    def initialize(tracer, name, attributes = {}, with_parent: nil, kind: :internal)
      @tracer = tracer
      @name = name
      @attributes = attributes
      @events = []
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
  end

  class Tracer
    attr_reader :spans

    def initialize
      @spans = []
      @stack = []
    end

    def start_span(name, attributes: {}, with_parent: nil, kind: :internal)
      parent = if with_parent.nil?
                 @stack.last
               else
                 with_parent
               end
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
      hierarchy = {}
      @spans.each do |span|
        if span.with_parent.nil?
          hierarchy[span.object_id] = span
        elsif (parent = hierarchy[span.with_parent.object_id])
          parent.nested << span
        else
          raise Error, "Parent span not found for span #{span.name}"
        end
      end
      hierarchy.values
    end
  end
end
