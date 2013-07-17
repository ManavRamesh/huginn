class SystemMailer < ActionMailer::Base
  default :from =>manav@blendlabsinc.com


  def send_message(options)
    @lines = options[:lines]
    @headline = options[:headline]
    mail :to => options[:to], :subject => options[:subject]
  end
end
