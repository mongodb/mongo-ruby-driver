# frozen_string_literal: true

require 'lite_spec_helper'
require 'tmpdir'
require 'json'

require File.expand_path('../../.evergreen/lib/coverage_gate', __dir__)

RSpec.describe CoverageGate do
  let(:tmpdir) { Dir.mktmpdir }
  let(:resultset_path) { File.join(tmpdir, 'coverage', '.resultset.json') }
  let(:baseline_path) { File.join(tmpdir, '.simplecov_baseline.json') }
  let(:project_root) { tmpdir }
  let(:output) { StringIO.new }

  let(:gate) do
    described_class.new(
      resultset_path: resultset_path,
      baseline_path: baseline_path,
      project_root: project_root,
      output: output
    )
  end

  before do
    FileUtils.mkdir_p(File.dirname(resultset_path))
  end

  after do
    FileUtils.remove_entry(tmpdir)
  end

  def write_resultset(files)
    coverage = files.transform_keys { |rel| File.join(project_root, rel) }
                    .transform_values { |lines| { 'lines' => lines } }
    File.write(resultset_path, JSON.dump('rspec' => { 'coverage' => coverage }))
  end

  def write_baseline(files)
    File.write(baseline_path, JSON.pretty_generate('files' => files))
  end

  describe '#check' do
    context 'when current coverage matches baseline' do
      it 'returns 0' do
        write_resultset('lib/mongo/foo.rb' => [ nil, 1, 1, 0, nil ])
        write_baseline('lib/mongo/foo.rb' => { 'covered' => 2, 'total' => 3 })
        expect(gate.check).to eq(0)
      end
    end

    context 'when a tracked file regresses' do
      it 'returns 1' do
        write_resultset('lib/mongo/foo.rb' => [ nil, 1, 0, 0, nil ])
        write_baseline('lib/mongo/foo.rb' => { 'covered' => 2, 'total' => 3 })
        expect(gate.check).to eq(1)
      end
    end

    context 'when a tracked file improves' do
      it 'returns 0' do
        write_resultset('lib/mongo/foo.rb' => [ nil, 1, 1, 1, nil ])
        write_baseline('lib/mongo/foo.rb' => { 'covered' => 2, 'total' => 3 })
        expect(gate.check).to eq(0)
      end
    end

    context 'when files outside lib/mongo are in the resultset' do
      it 'ignores them' do
        write_resultset(
          'lib/mongo/foo.rb' => [ nil, 1, 1, 0, nil ],
          'spec/some_spec.rb' => [ 1, 1, 1 ]
        )
        write_baseline('lib/mongo/foo.rb' => { 'covered' => 2, 'total' => 3 })
        expect(gate.check).to eq(0)
      end
    end

    context 'when a file is in the resultset but not the baseline' do
      it 'returns 0 and reports the file as new' do
        write_resultset('lib/mongo/new_file.rb' => [ nil, 1, 0, nil ])
        write_baseline({})
        expect(gate.check).to eq(0)
        expect(output.string).to include('new_file.rb')
        expect(output.string).to include('new')
      end
    end

    context 'when the baseline file does not exist' do
      it 'returns 0 (treats baseline as empty)' do
        write_resultset('lib/mongo/foo.rb' => [ nil, 1, 1, 0, nil ])
        # baseline_path intentionally not created
        expect(File).not_to exist(baseline_path)
        expect(gate.check).to eq(0)
      end
    end

    context 'when the resultset file does not exist' do
      it 'raises a clear error' do
        write_baseline('lib/mongo/foo.rb' => { 'covered' => 2, 'total' => 3 })
        # resultset_path intentionally not created
        expect { gate.check }.to raise_error(/SimpleCov did not produce a result/)
      end
    end

    context 'when a file is in the baseline but not the resultset' do
      it 'returns 0 and reports the file as missing' do
        write_resultset({})
        write_baseline('lib/mongo/deleted.rb' => { 'covered' => 5, 'total' => 5 })
        expect(gate.check).to eq(0)
        expect(output.string).to include('deleted.rb')
        expect(output.string).to include('missing')
      end
    end
  end
end
