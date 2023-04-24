# frozen_string_literal: true
# rubocop:todo all

require 'rubydns'

# Hack to stop the server - https://github.com/socketry/rubydns/issues/75
module Async
  class Task
    alias :run_without_record :run
    def run(*args)
      run_without_record.tap do
        $last_async_task = self
      end
    end
  end
end
