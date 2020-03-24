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
        example: $current_example,
      )
    end
  end

  def verify_empty!
    @lock.synchronize do
      alive_thread_records = @records.select { |record| record.thread.alive? }
      if alive_thread_records.any?
        msg = "Live background threads after closing all clients:"
        alive_thread_records.each do |record|
          msg << "\n  #{record.object}"
          if record.object.respond_to?(:options)
            msg << "\n  with options: #{record.object.options}"
          end
          msg << "\n  in #{record.example.id} #{record.example.full_description}"
        end
        raise msg
      end
      @records.clear
    end
  end
end

RSpec.configure do |config|
  config.around do |example|
    $current_example = example
    begin
      example.run
    ensure
      $current_example = nil
    end
  end
end
