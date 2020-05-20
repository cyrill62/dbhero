# frozen_string_literal: true

require 'csv'

module Dbhero
  class Dataclip < ActiveRecord::Base
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

    def query_result
      DataclipRead.transaction do
        begin
          @q_result ||= Rails.cache.fetch(self, expires_in: cache_ttl) do
            DataclipRead.connection.select_all(raw_query)
          end
        rescue StandardError => e
          errors.add(:base, e.message)
        end
        raise ActiveRecord::Rollback
      end
    end

    def csv_options
      {
        force_quotes: true,
        col_sep: Dbhero.csv_delimiter
      }
    end

    def csv_string
      query_result
      csv_string = CSV.generate(csv_options) do |csv|
        csv << @q_result.columns
        @q_result.rows.each { |row| csv << row }
      end
      csv_string
    end
  end
end
