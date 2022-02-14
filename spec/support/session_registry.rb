# frozen_string_literal: true
# encoding: utf-8

require 'singleton'

module Mongo
  class Client
    alias :get_session_without_tracking :get_session

    def get_session(options = {})
      get_session_without_tracking(options).tap do |session|
        SessionRegistry.instance.register(session) unless session.ended?
      end
    end
  end

  class Session
    alias :end_session_without_tracking :end_session

    def end_session
      SessionRegistry.instance.unregister(self)
      end_session_without_tracking
    end

    alias :materialize_without_tracking :materialize

    def materialize(connection)
      materialize_without_tracking(connection)
      SessionRegistry.instance.register(self)
    end
  end
end


class SessionRegistry
  include Singleton

  def initialize
    @registry = {}
    @mutex = Mutex.new
  end

  def register(session)
    @mutex.synchronize do
      @registry[session.session_id] = session if session
    end
  end

  def unregister(session)
    @mutex.synchronize do
      @registry.delete(session.session_id) unless session.ended?
    end
  end

  def verify_sessions_ended!
    @mutex.synchronize do
      @registry.delete_if { |_, session| session.ended? }

      unless @registry.empty?
        sessions = @registry.map { |_, session| session }
        raise "Session registry contains live sessions: #{sessions.join(', ')}"
      end
    end
  end

  def verify_single_session!
    @mutex.synchronize do
      @registry.delete_if { |_, session| session.ended? }

      unless @registry.size == 1
        sessions = @registry.map { |_, session| session.inspect }
        raise "Session registry contains live sessions: #{sessions.join(', ')}"
      end
    end
  end

  def clear_registry
    @registry = {}
  end
end
