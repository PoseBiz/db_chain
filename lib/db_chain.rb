require "bundler/setup"
require 'pg'
require 'fog'
require 'heroku-api'

class DbChain
  attr_reader :databases

  def self.from_heroku(app_name,opts={})
    db_chain = self.new(app_name,opts)
    db_chain.load_databases_from_heroku!
    db_chain
  end

  def self.from_sdb(app_name,opts={})
    db_chain = self.new(app_name,opts)
    db_chain.load_databases_from_sdb!
    db_chain
  end

  def initialize(app_name,opts={})
    @sdb_domain = opts[:sdb_domain]
    @app_name = app_name
    @databases = {}
    @sdb = Fog::AWS::SimpleDB.new(aws_access_key_id: opts[:sdb_access_key],
                                  aws_secret_access_key: opts[:sdb_secret_key])
  end

  def load_databases_from_sdb!
    databases = @sdb.select("SELECT * FROM #{@sdb_domain} WHERE app = '#{@app_name}' AND type IS NOT NULL ORDER BY type").body["Items"]
    @databases = databases.reduce({}) {|m,kv|
      name,props = kv
      m[name] = props.reduce({}) {|pm,pkv|
        pn,pv = pkv
        pm[pn.to_sym] = pv[0]
        pm
      }
      m
    }
  end

  def sync!
    save_to_sdb!
    remove_unused_from_sdb!
  end

  def master
    @databases.values.detect {|d| d[:type] == 'master'}
  end

  def followers
    @databases.values.select {|d| d[:type] == 'follower'}
  end

  def to_config(env)
    @databases.reduce({}) {|m,kv|
      name, props = kv
      key = props[:type] == 'master' ? env.to_s : "#{env}_slave_database_#{props[:color].downcase}"
      m[key] = props.reduce({}) {|m,kv| 
        k,v = kv
        if  [:adapter, :database, :username, :password, :host, :port, :color, :type].include?(k)
          m[k.to_s] = k == :port ? v.to_i : v
        end
        m
      }
      m
    }
  end

  def heroku_pg_info
    raw_pginfo = `heroku pg:info --app #{@app_name}`
    databases = {}
    
    cur_db = nil
    raw_pginfo.each_line do |line|
      line.chomp!
      if line =~ /\A=== ([A-Za-z0-9_-]+)(_URL)?/
        cur_db = {:app => @app_name,
          :name => $1,
          :color => $1.split('_').last,
          :adapter => 'postgresql'}
        databases[$1] = cur_db
      elsif line =~ /\AFollowing:?\s+(.*)\Z/
        cur_db[:following] = $1
      elsif line =~ /\AStatus:?\s+(.*)\Z/
        cur_db[:status] = $1
      end
    end
    databases.delete("SHARED_DATABASE")
    
    databases
  end

  def heroku_pg_connections
    heroku = Heroku::API.new(api_key: File.open("#{ENV['HOME']}/.heroku_key").read.chomp)
    heroku.get_config_vars(@app_name).
      body.
      reduce({}) {|dbs,kv|
      k,v = kv
      if k =~ /\A(HEROKU_POSTGRESQL_[A-Za-z0-9_-]+)_URL\Z/
        name = $1
        
        v =~ /\Apostgres:\/\/([A-Za-z0-9_\-]+):([A-Za-z0-9_\-]+)@([A-Za-z0-9_\-\.]+):(\d+)\/([A-Za-z0-9]+)/
        info = {
          username: $1,
          password: $2,
          host: $3,
          port: $4,
          database: $5
        }
        dbs[name] = info
      end
      dbs
    }
  end

  def retrieve_db_info
    puts "Retrieving DB info..."
    info = heroku_pg_info
    conns = heroku_pg_connections
    
    create_time = Time.now.to_s
    info.reduce({}) {|m,kv|
      name,props = kv
      props[:type] = props[:following] ? 'follower' : 'master'
      props[:created_at] = create_time
      
      
      if props[:name] == "SHARED_DATABASE" || props[:status] != 'available'
        puts "Skipping #{name} with status: #{props[:status]}"
      else
        sdb_key = "#{@app_name}_#{name}"
        m[sdb_key] = props.merge(conns[name])
      end
      m
    }
  end

  def load_databases_from_heroku!
    puts "Updating database config for #{@app_name}..."
    @databases = retrieve_db_info
  end

  def save_to_sdb!
    puts "Saving known good databases:"
    puts y @databases
    @sdb.batch_put_attributes(@sdb_domain, @databases, @databases)
    puts "Done saving known good databases" 
  end

  def remove_unused_from_sdb!
    good_db_keys = @databases.keys
    puts "Checking for unusable databases..."
    all_db_keys = @sdb.select("SELECT name FROM #{@sdb_domain} WHERE app = '#{@app_name}'").body["Items"].keys
    missing = all_db_keys.select {|k| !good_db_keys.include?(k)}
    if missing.empty?
      puts "No unusable databases needing @sdb deletion."
    else
      missing.each do |k|
        print "Deleting entry for unused DB '#{k}'..."
        @sdb.delete_attributes @sdb_domain, k
        puts "done"
      end
      puts "Finished removing unusable databases."
    end
  end

  def get_pg_extras!
    # Threaded to get more accurate numbers
    threads = []
    databases.each do |name,attrs|
      conn = PG::Connection.open({user: attrs[:username], password: attrs[:password], host: attrs[:host], sslmode: 'require', dbname: attrs[:database]})
      threads << Thread.new do
        txid = conn.exec("SELECT txid_current_snapshot()")[0]["txid_current_snapshot"]
        attrs[:txid_current_snapshot] = txid
        attrs[:txid_min] = txid.split(/\:/)[0].to_i
        attrs[:txid_max] = txid.split(/\:/)[1].to_i
        
        blk_stats = conn.exec("SELECT * FROM pg_stat_database where datname = '#{attrs[:database]}'")[0]

        %w(blks_hit blks_read tup_returned tup_fetched tup_inserted tup_updated tup_deleted).each {|k|
          attrs[k.to_sym] = blk_stats[k].to_i
        }

        attrs[:cache_hit_ratio] = attrs[:blks_hit].to_f / (attrs[:blks_hit] + attrs[:blks_read])

        attrs[:running_queries_count] = conn.exec("SELECT COUNT(*) AS count FROM pg_stat_activity  WHERE usename != 'collectd' AND query NOT LIKE '<%'")[0]["count"].to_i

        attrs[:slow_queries] = conn.exec("SELECT query FROM pg_stat_activity  WHERE usename != 'collectd' AND query NOT LIKE '<%' AND (now()-query_start) > '1 seconds' ORDER BY query_start ASC").map {|r| r["query"]}
        
        conn.close
      end
    end
    threads.each(&:join)
    
    master_txid_min = master[:txid_min]
    master[:txid_lag] = 0
    
    followers.each do |follower|
      follower[:txid_lag] =  master_txid_min - follower[:txid_min].to_i
    end

    databases
  end
end
