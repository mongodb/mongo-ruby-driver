# frozen_string_literal: true
# rubocop:todo all

require 'singleton'
require 'ostruct'

module Mongo
  module BackgroundThread

    alias :start_without_tracking! :start!

    def start!
      start_without_tracking!.tap do |thread|
        BackgroundThreadRegistry.instance.register(self, thread)
      end
    end
  end
end

class BackgroundThreadRegistry
  include Singleton

  def initialize
    @lock = Mutex.new
    @records = []
  end

  def register(object, thread)
    @lock.synchronize do
      @records << OpenStruct.new(
        thread: thread,
        object: object,
        # When rake spec:prepare is run, the current_example method is not defined
        example: RSpec.respond_to?(:current_example) ? RSpec.current_example : nil,
      )
    end
  end

  def verify_empty!
    @lock.synchronize do
      alive_thread_records = @records.select { |record| record.thread.alive? }
      if alive_thread_records.any?
        msg = +"Live background threads after closing all clients:"
        alive_thread_records.each do |record|
          msg << "\n  #{record.object}"
          if record.object.respond_to?(:options)
            msg << "\n  with options: #{record.object.options}"
          end
          if record.example
            msg << "\n  in #{record.example.id}: #{record.example.full_description}"
          else
            msg << "\n  not in an example"
          end
        end
        raise msg
      end
      @records.clear
    end
  end
end
