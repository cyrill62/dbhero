# frozen_string_literal: true

class Dbhero::ApplicationController < ActionController::Base
  def check_auth
    if Dbhero.authenticate && !(dbhero_current_user && call_custom_auth)
      raise ActionController::RoutingError,
            'Forbidden'
    end
  end

  def user_representation
    dbhero_current_user&.send(Dbhero.user_representation)
  end

  def dbhero_current_user
    send(Dbhero.current_user_method) if Dbhero.authenticate && Dbhero.current_user_method.present?
  end

  def call_custom_auth
    cond = Dbhero.custom_user_auth_condition
    if cond.present? && cond.is_a?(Proc)
      cond.call(dbhero_current_user)
    else
      true
    end
  end
end
