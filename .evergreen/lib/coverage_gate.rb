# frozen_string_literal: true

require 'json'
require 'time'

# Compares a SimpleCov resultset against a checked-in per-file baseline and
# fails if any tracked file's line-coverage ratio decreased.
class CoverageGate
  Entry = Struct.new(
    :path,
    :baseline_covered, :baseline_total,
    :current_covered, :current_total,
    :status,
    keyword_init: true
  )

  TRACKED_PREFIX = 'lib/mongo/'

  def initialize(resultset_path:, baseline_path:, project_root: Dir.pwd, output: $stdout)
    @resultset_path = resultset_path
    @baseline_path = baseline_path
    @project_root = project_root
    @output = output
  end

  # Returns 0 if no regressions, 1 otherwise.
  def check
    entries = compare(load_current, load_baseline)
    @output.puts(format_report(entries))
    (entries.any? { |e| e.status == :regression }) ? 1 : 0
  end

  # Writes the current resultset back to the baseline path. Used by developers
  # to lock in an intentional coverage change.
  def update_baseline
    File.write(@baseline_path, "#{format_baseline(load_current)}\n")
    0
  end

  # Like #check, but always returns 0. Used for local inspection.
  def report
    entries = compare(load_current, load_baseline)
    @output.puts(format_report(entries))
    0
  end

  private

  def format_baseline(current)
    files = current.sort.to_h.transform_values do |v|
      { 'covered' => v[:covered], 'total' => v[:total] }
    end

    JSON.pretty_generate(
      'generated_at' => Time.now.utc.iso8601,
      'ruby_version' => RUBY_VERSION,
      'files' => files
    )
  end

  def load_current
    unless File.exist?(@resultset_path)
      raise 'SimpleCov did not produce a result; was COVERAGE=1 set? ' \
            "(looked for #{@resultset_path})"
    end

    parse_resultset(JSON.parse(File.read(@resultset_path)))
  end

  def parse_resultset(data)
    _, run = data.first
    coverage = run.fetch('coverage')
    coverage.each_with_object({}) do |(abs_path, file_data), out|
      rel = relative_path(abs_path)
      next unless rel

      lines = file_data.is_a?(Hash) ? file_data['lines'] : file_data
      out[rel] = count_lines(lines)
    end
  end

  def load_baseline
    return { 'files' => {} } unless File.exist?(@baseline_path)

    JSON.parse(File.read(@baseline_path))
  end

  def relative_path(abs_path)
    prefix = "#{@project_root}/"
    return nil unless abs_path.start_with?(prefix)

    rel = abs_path.sub(prefix, '')
    rel.start_with?(TRACKED_PREFIX) ? rel : nil
  end

  def count_lines(line_hits)
    relevant = line_hits.compact
    {
      covered: relevant.count { |c| c.is_a?(Integer) && c.positive? },
      total: relevant.size,
    }
  end

  def compare(current, baseline)
    files = baseline.fetch('files', {})
    keys = (current.keys + files.keys).uniq.sort
    keys.map { |path| build_entry(path, current[path], files[path]) }
  end

  def build_entry(path, cur, base)
    if cur && base
      Entry.new(
        path: path,
        baseline_covered: base['covered'], baseline_total: base['total'],
        current_covered: cur[:covered], current_total: cur[:total],
        status: regression?(cur, base) ? :regression : :ok
      )
    elsif cur
      Entry.new(
        path: path, baseline_covered: nil, baseline_total: nil,
        current_covered: cur[:covered], current_total: cur[:total],
        status: :new
      )
    else
      Entry.new(
        path: path,
        baseline_covered: base['covered'], baseline_total: base['total'],
        current_covered: nil, current_total: nil,
        status: :missing
      )
    end
  end

  def regression?(cur, base)
    cur[:covered] * base['total'] < base['covered'] * cur[:total]
  end

  def format_report(entries)
    header = 'file                                                  baseline     current  status'
    rows = entries.map do |e|
      format(
        '%-50s  %10s  %10s  %s',
        e.path,
        format_pct(e.baseline_covered, e.baseline_total),
        format_pct(e.current_covered, e.current_total),
        e.status
      )
    end
    ([ header ] + rows).join("\n")
  end

  def format_pct(covered, total)
    return '-' if covered.nil? || total.nil? || total.zero?

    format('%.1f%%', covered * 100.0 / total)
  end
end
