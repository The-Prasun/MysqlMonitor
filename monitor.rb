require 'mysql'
require 'mail'
require 'erb'

# ADD DB CREDENTIALS
db_host = 'localhost'
db_user = 'root'
db_pw = ''
db_db = 'information_schema'

# SMTP CREDENTIALS
options = { :address              => "smtp.gmail.com",
            :port                 => 587,
            :domain               => 'domainname',
            :user_name            => 'username',
            :password             => 'password',
            :authentication       => 'plain',
            :enable_starttls_auto => true  }

# Comparison Threshold
threshold = 1.2


db = Mysql.new(db_host, db_user, db_pw, db_db)

# Wrap sql calls in block to ensure connection to db is closed.
begin
  # GET RECORDS FROM information_schema db
  info_results = db.query "SELECT table_schema AS db_name,
      table_name AS tbl_name,
      table_rows AS num_rows
      FROM information_schema.tables WHERE table_rows IS NOT NULL
      AND table_schema <> 'mysql'"

  # GET RECORDS FROM current_size db
  old_results = db.query "SELECT db_name, tbl_name, num_rows FROM current_size.info"

  # IF information_schema GREATER BY SOME THRESHOLD (20%), ADD TO SET size_issues
  size_issues = []

  # ONCE DONE COMPARING, UPDATE current_size RECORDS WITH information_schema RESULTS
  info_results.each_hash do |row|
    old_results.each_hash do |old|
      if row['db_name'] == old['db_name'] && row['tbl_name'] == old['tbl_name']
        if row['num_rows'].to_f >= threshold * old['num_rows'].to_f
          size_issues << {
            db_name: row['db_name'],
            tbl_name: row['tbl_name'],
            cur_rows: row['num_rows'],
            old_rows: old['num_rows'] }
        end
      end
    end
    st = db.prepare("INSERT INTO current_size.info
        (`db_name`, `tbl_name`)
        VALUES (?, ?)
        ON DUPLICATE KEY UPDATE `num_rows` = ?")
    st.execute(row['db_name'], row['tbl_name'], row['num_rows'])
  end
  info_results.free
  old_results.free
ensure
  db.close
end

# EMAIL OUT LIST size_issues
unless size_issues.empty?
  email_template = ERB.new(File.read('_mail.html.erb'))

  Mail.deliver do
    delivery_method :smtp, options
    to      'toemail'
    from    'fromemail'
    subject 'MySQL tables growing faster than normal'

    html_part do
      content_type 'text/html; charset=UTF-8'
      body email_template.result(binding)
    end
  end
end
