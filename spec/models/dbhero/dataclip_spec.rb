# frozen_string_literal: true

require 'rails_helper'

RSpec.describe Dbhero::Dataclip, type: :model do
  context 'Validations' do
    it { is_expected.to validate_presence_of(:description) }
    it { is_expected.to validate_presence_of(:raw_query) }
  end

  context 'before create' do
    describe '.set_token' do
      subject { build(:dataclip) }

      it 'token should be nil when clip is not persisted' do
        expect(subject.token).to be_nil
      end

      it 'token should be present after create clip' do
        subject.save
        expect(subject.token).not_to be_nil
      end
    end
  end

  describe '.ordered' do
    subject { described_class.ordered }

    let!(:clip1) { create(:dataclip, updated_at: 2.days.ago) }
    let!(:clip2) { create(:dataclip, updated_at: 1.day.ago) }
    let!(:clip3) { create(:dataclip, updated_at: 4.days.ago) }

    it do
      expect(subject).to eq([clip2, clip1, clip3])
    end
  end

  describe '#to_param' do
    subject { dataclip.to_param }

    let(:dataclip) { create(:dataclip) }

    it { is_expected.to eq(dataclip.slug) }
  end

  describe '#title' do
    subject { dataclip.title }

    let(:dataclip) { create(:dataclip, description: "title\ndescription\nfoo") }

    it { is_expected.to eq('title') }
  end

  describe '#description_without_title' do
    subject { dataclip.description_without_title }

    let(:dataclip) { create(:dataclip, description: "title\ndescription\nfoo") }

    it { is_expected.to eq("description\nfoo") }
  end

  describe '#csv_string' do
    subject { dataclip.csv_string }

    let(:dataclip) do
      create(:dataclip,
             raw_query: "select 'foo'::text as bar, 'bar'::text as foo")
    end

    it { is_expected.to eq(%("bar","foo"\n"foo","bar"\n)) }
  end

  describe '#total_rows' do
    subject { dataclip.total_rows }

    let(:dataclip) do
      create(:dataclip,
             raw_query: 'select foo.nest from (select unnest(ARRAY[1,2,3]) as nest) foo')
    end

    before { dataclip.query_result }

    it { is_expected.to eq(3) }
  end

  describe '#query_result' do
    context 'executes raw_query and return they result on q_result' do
      subject { dataclip.q_result }

      let(:dataclip) do
        create(:dataclip,
               raw_query: "select 'foo'::text as bar, 'bar'::text as foo")
      end

      before { dataclip.query_result }

      it 'is kind of ActiveRecord::Result' do
        expect(subject).to be_an_instance_of(ActiveRecord::Result)
      end

      it 'explore on result set' do
        expect(subject.columns).to eq(%w[bar foo])
        expect(subject.rows).to eq([%w[foo bar]])
      end
    end

    context 'test some security' do
      context 'with truncate' do
        let(:dataclip) do
          create(:dataclip, raw_query: 'TRUNCATE table dbhero_dataclips')
        end

        before do
          create_list(:dataclip, 5)
        end

        it 'does not truncate table dataclips' do
          dataclip.query_result
          expect(described_class.count).to eq(6)
        end
      end

      context 'with commit' do
        let(:dataclip) do
          create(:dataclip,
                 raw_query: 'TRUNCATE table dbhero_dataclips; commit;')
        end

        before do
          create_list(:dataclip, 5)
        end

        it 'does not truncate table dataclips' do
          dataclip.query_result
          expect(described_class.count).to eq(6)
          expect(dataclip.errors.full_messages.to_sentence.match(/(PG::SyntaxError: ERROR)/)).not_to be_nil
        end
      end
    end
  end
end
