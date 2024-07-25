# frozen_string_literal: true

require 'csv'

class Dbhero::Dataclip < ApplicationRecord
  before_create :set_token_and_slug
  after_save :refresh_cache

  scope :ordered, -> { order(updated_at: :desc) }
  scope :desc_search, lambda { |term|
    where(arel_table[:description].matches("%#{term}%"))
  }

  validates :description, :raw_query, presence: true
  attr_reader :q_result

  def refresh_cache
    Rails.cache.delete(self)
  end

  def set_token_and_slug
    self.token = SecureRandom.uuid unless token
    self.slug = SecureRandom.uuid unless slug
  end

  def to_param
    slug
  end

  def title
    description.split("\n")[0]
  end

  def description_without_title
    description.split("\n")[1..-1].join("\n")
  end

  def total_rows
    @total_rows ||= @q_result.rows.length
  end

  def cached?
    @cached ||= Rails.cache.fetch(self).present?
  end

  def cache_ttl
    (::Dbhero.cached_query_exp || 10.minutes)
  end

  def query_result(params = {})
    cached_key = "#{cache_key}/" \
                 "#{params.to_a.flatten.join('-').parameterize}"

    query = raw_query

    params.each do |name, value|
      query.gsub!(
        /-?-?(.*)#{name.upcase}/,
        "\\1#{::Dbhero::DataclipRead.connection.quote(value)}",
      )
    end

    ::Dbhero::DataclipRead.transaction do
      begin
        @q_result ||= Rails.cache.fetch(cached_key, expires_in: cache_ttl) do
          ::Dbhero::DataclipRead.connection.select_all(query)
        end
      rescue StandardError => e
        errors.add(:base, e.message)

        raise ActiveRecord::Rollback
      end
    end
  end

  def csv_options
    {
      force_quotes: true,
      col_sep: Dbhero.csv_delimiter,
    }
  end

  def csv_string(params)
    query_result(params)
    CSV.generate(csv_options) do |csv|
      csv << @q_result.columns
      @q_result.rows.each { |row| csv << row }
    end
  end
end
