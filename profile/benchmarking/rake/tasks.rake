# frozen_string_literal: true

require_relative '../../benchmarking'

# Some require data files, available from the drivers team.
# See the comments above each task for details.
namespace :benchmark do
  %w[ bson single_doc multi_doc parallel ].each do |group|
    load File.join(__dir__, "#{group}.rake")
  end
end
