# frozen_string_literal: true
# encoding: utf-8

require 'singleton'

module Mongo
  class Client
    alias :get_session_without_tracking :get_session

    def get_session(options = {})
      get_session_without_tracking(options).tap do |session|
        SessionRegistry.instance.register(session)
      end
    end
  end

  class Session
    alias :end_session_without_tracking :end_session

    def end_session
      SessionRegistry.instance.unregister(self)
      end_session_without_tracking
    end
  end
end


class SessionRegistry
  include Singleton

  def initialize
    @registry = {}
  end

  def register(session)
    @registry[session.session_id] = session if session
  end

  def unregister(session)
    @registry.delete(session.session_id) unless session.ended?
  end

  def verify_sessions_ended!
    @registry.delete_if { |_, session| session.ended? }

    unless @registry.empty?
      sessions = @registry.map { |_, session| session }
      raise "Session registry contains live sessions: #{sessions.join(', ')}"
    end
  end

  def clear_registry
    @registry = {}
  end
end
