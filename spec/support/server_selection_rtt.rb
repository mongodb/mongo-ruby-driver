module Mongo
  module ServerSelection
    module RTT

      # Represents a specification.
      #
      # @since 2.0.0
      class Spec

        # @return [ String ] description The spec description.
        attr_reader :description

        # @return [ Float ] avg_rtt_ms The starting average round trip time.
        attr_reader :avg_rtt_ms

        # @return [ Float ] new_rtt_ms The new round trip time for ismaster.
        attr_reader :new_rtt_ms

        # @return [ Float ] new_avg_rtt The newly calculated moving average round trip time.
        attr_reader :new_avg_rtt

        # Instantiate the new spec.
        #
        # @example Create the spec.
        #   Spec.new(file)
        #
        # @param [ String ] file The name of the file.
        #
        # @since 2.0.0
        def initialize(file)
          @test = YAML.load(ERB.new(File.new(file).read).result)
          @description = "avg_rtt_ms: #{@test['avg_rtt_ms']}, new_rtt_ms: #{@test['new_rtt_ms']}," +
                           " new_avg_rtt: #{@test['new_avg_rtt']}"
          @avg_rtt_ms = @test['avg_rtt_ms'] == 'NULL' ? nil : @test['avg_rtt_ms'].to_f
          @new_rtt_ms = @test['new_rtt_ms'].to_f
          @new_avg_rtt = @test['new_avg_rtt'].to_f
        end
      end
    end
  end
end
