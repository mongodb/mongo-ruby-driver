# frozen_string_literal: true

module Tracing
  class Span

    attr_reader :name, :attributes, :events, :with_parent, :kind, :finished

    attr_accessor :status

    def initialize(name, attributes = {}, with_parent: nil, kind: :internal)
      @name = name
      @attributes = attributes
      @events = []
      @with_parent = with_parent
      @kind = kind
      @finished = false
    end

    def set_attribute(key, value)
      @attributes[key] = value
    end

    def add_event(name, attributes: {})
      event_attributes = { 'event.name' => name }
      event_attributes.merge!(attributes) unless attributes.nil?
      @events << event_attributes
    end

    def record_exception(exception, attributes: nil)
      event_attributes = {
        'exception.type' => exception.class.to_s,
        'exception.message' => exception.message,
        'exception.stacktrace' => exception.full_message(highlight: false, order: :top).encode('UTF-8', invalid: :replace, undef: :replace, replace: '�')
      }
      event_attributes.merge!(attributes) unless attributes.nil?
      add_event('exception', attributes: event_attributes)
    end

    def finish
      @finished = true
    end
  end

  class Tracer

    attr_reader :spans

    def initialize
      @spans = []
    end
    def in_span(name, attributes: {}, kind: :internal)
      span = Span.new(name, attributes, kind: kind)
      @spans << span
      context = Object.new
      yield(span, context) if block_given?
    end

    def start_span(name, attributes: {}, with_parent: nil, kind: :internal)
      Span.new(name, attributes, with_parent: with_parent, kind: kind).tap do |span|
        @spans << span
      end
    end
  end
end
