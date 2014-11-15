require 'net/http'
require 'csv'
require 'redis'
require 'date'
require 'json'

PAGE_COUNTS_FILE_URL_TEMPLATE = 'https://dumps.wikimedia.org/other/pagecounts-raw/%{year}/%{year}-%{month}/pagecounts-%{year}%{month}%{day}-%{hour}0000.gz'
MIN_YEAR = 2014
HOURS_PER_DAY = 23
CSV_FILE_NAME = 'wiki_encoded_urls2.csv'
REDIS_DB_NAME = 'wikipedia_pagecounts'
REDIS_IP = '127.0.0.1'
SLEEP_SECONDS = 120

def main
  date_start = Date.parse(ARGV[0])
  abort("[ERROR] Start date cannot be less than #{MIN_YEAR} year.") if date_start.year < MIN_YEAR
  if ARGV[1]
    date_end = Date.parse(ARGV[1]) 
    abort('[ERROR] Start date cannot be more than End date.') if date_start > date_end
  end 
  
  abort('[ERROR] Start date has to be less or equal to the current date.') if date_start >= DateTime.now.to_date
  to_date = date_end ? date_end : date_start
  (0..(to_date - date_start).to_i).to_a.each do |x|
    date_iter = date_start + x
    puts("- [Date] #{date_iter.strftime('%Y %m %d')}")
    month_formatted = '%02d' % date_iter.month
    day_formatted = '%02d' % date_iter.day
    
    local_file_names = (0..HOURS_PER_DAY).map do |hour|
      hour_formatted = '%02d' % hour
      puts(" - [Time] #{hour_formatted}:00")
      url = PAGE_COUNTS_FILE_URL_TEMPLATE % { year: date_iter.year, month: month_formatted, day: day_formatted, hour: hour_formatted }
      save_file(url)
    end

    local_file_names.each do |full_file_name|
      puts(" - [Processing] the file #{File.basename(full_file_name)}...")
      page_counts_str = uncompress_gz_file(full_file_name)
      page_counts_hash = process_gz_file_content(page_counts_str)

      source_links = read_and_parse_csv(CSV_FILE_NAME)
      date_iter_formatted = date_iter.strftime("%Y%m%d").to_sym
      process_pagecounts_dump_file(source_links, page_counts_hash, date_iter_formatted)
      
      puts(" - [OK]\n\n")
    end
  end

  puts('- [OK] Done')
end

def save_file(url, repeat_count = 3)
  uri = URI.parse(url)
  file_name = File.basename(uri.path)
  full_path = File.join(Dir.pwd, file_name)
  http = Net::HTTP.new(uri.host, uri.port)
  http.use_ssl = true
  http.request_get(uri.path) do |response|
    case response
    when Net::HTTPNotFound
      puts(' - [Error] 404: not found')
      return nil
    when Net::HTTPForbidden
      raise ' - [Error] 403: forbidden, exhausted all retry attempts' if repeat_count <= 0 
      puts('- [Error] 403: forbidden, one more attempt...')
      sleep(SLEEP_SECONDS)  
      return save_file(url, repeat_count - 1)
    when Net::HTTPClientError
      puts(" - [Error] client error: #{response.inspect}")
      return nil
    when Net::HTTPOK
      temp_file = Tempfile.new("download-#{file_name}")
      temp_file.binmode
      size = 0
      progress = 0
      total = response.header['Content-Length'].to_i
      total_mb = total / 1024 / 1024
      response.read_body do |chunk|
        temp_file << chunk
        size += chunk.size
        new_progress = (size * 100) / total
        print("\r - [Downloading] the dump file %s %3d%% of ≈ #{total_mb}Mb " % [file_name, new_progress]) unless new_progress == progress
        progress = new_progress
      end

      puts("[OK]")
      temp_file.close
      File.unlink(full_path) if File.exists?(full_path)
      FileUtils.mkdir_p(File.dirname(full_path))
      FileUtils.mv(temp_file.path, full_path, force: true)
    end
  end

  full_path
rescue Exception => e
  File.unlink(full_path) if File.exists?(full_path)
  puts(" - [Error]: #{e.message}")
  raise "Failed to download the file #{url}"
end

def read_and_parse_csv(csv_file_name)
  language_first_index = 7
  result = {}
  CSV.foreach(csv_file_name) do |row|
    first_url_index = row.index { |x| x.start_with?('http://') }
    row.drop(first_url_index).each do |maybe_link|
      link_result = {}
      
      if maybe_link.start_with?('http://')
        lng = maybe_link[language_first_index..language_first_index + 1]
        title = maybe_link[maybe_link.rindex('/') + 1..-1]
        title_formatted = parse_csv_row_title(title)
        key = "#{lng}_#{title_formatted}"
        result[key] = row[0]
      else
        prev_key = result.keys[-1]
        new_key = prev_key + ',' + maybe_link
        result.delete(prev_key)
        result[new_key] = row[0]
      end

    end
  end

  result
end

def parse_csv_row_title(title)
  ind = title.index('?curid=')
  ind ? title[ind..title.length] : title
end

def connect_to_redis
  @redis ||= Redis.new(host: REDIS_IP, port: 6379, db: REDIS_DB_NAME)
end

def uncompress_gz_file(file_name)
  print(' - [Uncompressing] the dump file ')
  infile = open(file_name)
  gz = Zlib::GzipReader.new(infile)
  res = gz.read
  gz.close
  puts('[OK]')
  res
end

def process_gz_file_content(input)
  title_starting_index = 3
  print(' - [Parsing1] ')
  res = {}
  input.each_line do |x|
    lng = x[0..1].downcase
    ind1 = x.index(' ', title_starting_index)
    title = x[title_starting_index...ind1]
    key = "#{lng}_#{title}"
    ind2 = x.index(' ', ind1 + 1)
    val = x[ind1 + 1 ... ind2]
    res[key] = val
  end

  puts('[OK]')
  res
end

def process_pagecounts_dump_file(input, page_counts_hash, date_key)
  print(' - [Parsing2] ')
  result = {}
  input.each do |lng_title, intern_id|
    intern_id_sym = intern_id.to_sym
    if page_counts_hash[lng_title]
      count = page_counts_hash[lng_title].to_i
      lng = lng_title[0..1].to_sym
      if result.key?(intern_id_sym)
        if result[intern_id_sym].key?(date_key)
          if result[intern_id_sym][date_key].key?(lng)
            result[intern_id_sym][date_key][lng] += count
          else
            result[intern_id_sym][date_key][lng] = count
          end
        else
          result[intern_id_sym][date_key] = {}
          result[intern_id_sym][date_key][lng] = 0
        end
      else
        result[intern_id_sym] = {}
        result[intern_id_sym][date_key] = {}
        result[intern_id_sym][date_key][lng] = 0
      end
    end
  end

  puts('[OK]')
  save_to_redis(result)
end

def save_to_redis(input)
  print(' - [Saving] to redis ')
  redis = connect_to_redis()
  i = 0
  input.each do |k, date_lng_val|
    maybe_json = redis.get(k)
    if maybe_json
      old_data = JSON.parse(maybe_json, { symbolize_names: true })
      new_data = old_data.merge(date_lng_val) do |key, v1, v2| 
        v1.merge(v2) { |key, v1, v2| v1 + v2 }
      end

      redis.set(k, new_data.to_json) 
    else
      redis.set(k, date_lng_val.to_json)
    end

    i += 1
  end

  puts("[OK] (#{i} rows affected)")
end

main()