# frozen_string_literal: true

require_dependency 'dbhero/application_controller'
require_dependency 'responders'
require_dependency 'has_scope'

class Dbhero::DataclipsController < Dbhero::ApplicationController
  before_action :check_auth, except: [:show]
  before_action :set_dataclip, only: %i[show edit update destroy]
  has_scope :desc_search
  respond_to :html, :csv

  def index
    @dataclips = apply_scopes(Dbhero::Dataclip.ordered)
  end

  def show
    check_auth if @dataclip.private?
    query_params = if params.key?(:query)
                     params.require(:query).permit!.to_h
                   else
                     {}
                   end

    @dataclip.query_result(query_params)

    respond_to do |format|
      format.html do
        return render :show, layout: false if request.xhr?
      end

      format.csv do
        send_data @dataclip.csv_string(query_params),
                  type: Mime[:csv],
                  disposition: "attachment; filename=#{@dataclip.token}.csv"
      end
    end
  end

  def new
    @dataclip = Dbhero::Dataclip.new
  end

  def edit
    @dataclip.query_result
  end

  def create
    @dataclip = Dbhero::Dataclip.create(dataclip_params.merge(user: user_representation))
    respond_with @dataclip, location: edit_dataclip_path(@dataclip),
                            notice: 'Dataclip was successfully created.'
  end

  def update
    @dataclip.update(dataclip_params)
    respond_with @dataclip, location: edit_dataclip_path(@dataclip),
                            notice: 'Dataclip was successfully updated.'
  end

  def destroy
    @dataclip.destroy
    redirect_to dataclips_url, notice: 'Dataclip was successfully destroyed.'
  end

  private

  # Use callbacks to share common setup or constraints between actions.
  def set_dataclip
    @dataclip = Dbhero::Dataclip.find_by(id: params[:id]) ||
                Dbhero::Dataclip.find_by(token: params[:id]) ||
                Dbhero::Dataclip.find_by(slug: params[:id])

    check_auth unless @dataclip.token == params[:'access-token']
  end

  # Only allow a trusted parameter "white list" through.
  def dataclip_params
    params.require(:dataclip).permit(:description, :raw_query, :private)
  end
end
