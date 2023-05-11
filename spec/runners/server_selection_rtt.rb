# frozen_string_literal: true
# rubocop:todo all

module Mongo
  module ServerSelection
    module RTT

      # Represents a specification.
      #
      # @since 2.0.0
      class Spec

        # @return [ String ] description The spec description.
        attr_reader :description

        # @return [ Float ] average_rtt The starting average round trip time, in seconds.
        attr_reader :average_rtt

        # @return [ Float ] new_rtt The new round trip time for hello, in seconds.
        attr_reader :new_rtt

        # @return [ Float ] new_average_rtt The newly calculated moving average round trip time, in seconds.
        attr_reader :new_average_rtt

        # Instantiate the new spec.
        #
        # @param [ String ] test_path The path to the file.
        #
        # @since 2.0.0
        def initialize(test_path)
          @test = ::Utils.load_spec_yaml_file(test_path)
          @description = "#{File.basename(test_path)}: avg_rtt_ms: #{@test['avg_rtt_ms']}, new_rtt_ms: #{@test['new_rtt_ms']}," +
                           " new_avg_rtt: #{@test['new_avg_rtt']}"
          @average_rtt = @test['avg_rtt_ms'] == 'NULL' ? nil : @test['avg_rtt_ms'].to_f / 1000
          @new_rtt = @test['new_rtt_ms'].to_f / 1000
          @new_average_rtt = @test['new_avg_rtt'].to_f / 1000
        end
      end
    end
  end
end
