require 'mysql'

# ADD DB CREDENTIALS
db = Mysql.new('localhost', 'root', '', 'information_schema')

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
    puts "Table #{row['db_name']}: #{row['tbl_name']} (#{row['num_rows']})"
    old_results.each_hash do |old|
      if row['db_name'] == old['db_name'] && row['tbl_name'] == old['tbl_name']
        if row['num_rows'].to_f >= 1.2 * old['num_rows'].to_f
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
