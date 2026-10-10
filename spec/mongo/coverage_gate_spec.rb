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

  def write_multi_session_resultset(sessions)
    payload = sessions.each_with_index.with_object({}) do |(files, idx), out|
      coverage = files.transform_keys { |rel| File.join(project_root, rel) }
                      .transform_values { |lines| { 'lines' => lines } }
      out["rspec-#{idx}"] = { 'coverage' => coverage }
    end
    File.write(resultset_path, JSON.dump(payload))
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

    context 'when the resultset has multiple sessions (parallel buckets)' do
      it 'merges line hits across sessions before comparing' do
        write_multi_session_resultset(
          [
            # bucket A covers lines 1 and 2
            { 'lib/mongo/foo.rb' => [ nil, 1, 1, 0, nil ] },
            # bucket B covers line 3 (but not 1 or 2)
            { 'lib/mongo/foo.rb' => [ nil, 0, 0, 1, nil ] },
          ]
        )
        write_baseline('lib/mongo/foo.rb' => { 'covered' => 3, 'total' => 3 })
        expect(gate.check).to eq(0)
      end

      it 'treats a nil from any session as non-executable' do
        # Session A loaded the file: Ruby Coverage marked lines 0, 4, 5 as
        # non-executable (nil). Session B used the track_files heuristic and
        # tagged those same lines as executable-not-hit (0). The merge must
        # not inflate total by counting heuristic 0s where a real run said nil.
        write_multi_session_resultset(
          [
            { 'lib/mongo/foo.rb' => [ nil, 1, 1, 0, nil, nil ] },
            { 'lib/mongo/foo.rb' => [ 0,   0, 0, 0, 0,   0   ] },
          ]
        )
        write_baseline('lib/mongo/foo.rb' => { 'covered' => 2, 'total' => 3 })
        expect(gate.check).to eq(0)
      end
    end
  end

  describe '#update_baseline' do
    it 'writes the current resultset to the baseline path' do
      write_resultset('lib/mongo/foo.rb' => [ nil, 1, 1, 0, nil ])
      gate.update_baseline
      expect(File).to exist(baseline_path)
      data = JSON.parse(File.read(baseline_path))
      expect(data['files']).to eq('lib/mongo/foo.rb' => { 'covered' => 2, 'total' => 3 })
    end

    it 'records ruby version and generation timestamp' do
      write_resultset('lib/mongo/foo.rb' => [ 1, 1 ])
      gate.update_baseline
      data = JSON.parse(File.read(baseline_path))
      expect(data['ruby_version']).to eq(RUBY_VERSION)
      expect(data['generated_at']).to match(/\A\d{4}-\d{2}-\d{2}T/)
    end

    it 'sorts file keys for diff stability' do
      write_resultset(
        'lib/mongo/zeta.rb' => [ 1, 1 ],
        'lib/mongo/alpha.rb' => [ 1, 1 ]
      )
      gate.update_baseline
      raw = File.read(baseline_path)
      expect(raw.index('alpha.rb')).to be < raw.index('zeta.rb')
    end
  end

  describe '#report' do
    it 'always returns 0 even when there is a regression' do
      write_resultset('lib/mongo/foo.rb' => [ nil, 1, 0, 0, nil ])
      write_baseline('lib/mongo/foo.rb' => { 'covered' => 2, 'total' => 3 })
      expect(gate.report).to eq(0)
      expect(output.string).to include('foo.rb')
    end
  end
end
