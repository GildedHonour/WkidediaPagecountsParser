require 'net/http'
require 'csv'
require 'redis'
require 'date'
require 'pry' #todo remove
require 'json'

PAGE_COUNTS_FILE_URL_TEMPLATE = 'https://dumps.wikimedia.org/other/pagecounts-raw/%{year}/%{year}-%{month}/pagecounts-%{year}%{month}%{day}-%{hour}0000.gz'
MIN_YEAR = 2007
HOURS_PER_DAY = 23
CSV_FILE_NAME = 'wiki_urls.csv'
REDIS_DB_NAME = 'wikipedia_pagecounts'
REDIS_LOGIN = ''
REDIS_PASSWORD = ''
REDIS_IP = ''

def main
  date_start = Date.parse(ARGV[0])
  abort("[ERROR] Start date cannot be let than #{MIN_YEAR} year.") if date_start.year < MIN_YEAR
  if ARGV[1]
    date_end = Date.parse(ARGV[1]) 
    abort('[ERROR] Start date cannot be more than End date.') if date_start > date_end
  end 
  
  abort('[ERROR] Start date has to be less or equal to the current date.') if date_start >= DateTime.now.to_date


  to_date = date_end ? date_end : date_start
  (0..(to_date - date_start).to_i).to_a.each do |x|
    date_iter = date_start + x

    
    # local_file_names = (0..HOURS_PER_DAY).map do |hour|
    local_file_names = (0..2).map do |hour| #todo debug
      hour_formatted = '%02d' % hour
      file_url = PAGE_COUNTS_FILE_URL_TEMPLATE % { year: date_iter.year, month: date_iter.month, day: date_iter.day, hour: hour_formatted }
      local_file_name = 'pagecounts-%{year}%{month}%{day}-%{hour}0000.gz' % { year: date_iter.year, month: date_iter.month, day: date_iter.day, hour: hour } #todo - take the part after /
      save_file(file_url, local_file_name)
      local_file_name
    end

    binding.pry

    local_file_names.each do |file_name|
      puts(" - Processing #{file_name}...")
      page_counts_str = uncompress_gz_file("/Users/alex/Downloads/#{file_name}") #todo - path
      page_counts_hash = process_gz_file_content(page_counts_str)

      source_links = read_and_parse_csv(CSV_FILE_NAME)
      date_iter_formatted = date_iter.strftime("%Y%m%d").to_sym
      process_pagecounts_dump_file(source_links, page_counts_hash, date_iter_formatted)
      puts("Ok! Done processing #{file_name}")
    end
  end
end


def save_file(url, local_file_name)
  puts(" - Downloading #{url} ...")
  uri = URI.parse(url)
  http = Net::HTTP.new(uri.host, uri.port)
  http.use_ssl = true
  data = http.get(uri.request_uri)
  if data.code.to_i == 200
    File.write(local_file_name, data.body)
    puts(" -- [OK]\n\n")
  else
    puts(" -- [Error] #{data.code}, #{data.message}\n\n")
  end
end

def read_and_parse_csv(csv_file_name)
  result = {}
  CSV.foreach(csv_file_name) do |row|
    first_url_index = row.index { |x| x.start_with?('http://') }
    row.drop(first_url_index).each do |maybe_link|
      link_result = {}
      if maybe_link.start_with?('http://')
        lng = maybe_link[7..8] #todo - hardcode
        title = maybe_link[maybe_link.rindex('/') + 1..-1]
        next if title.include?('?curid=') #todo - /index.html?curid=91365 - take only ?curid=91365
        key = "#{lng}_#{title}"
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

def connect_to_redis
  Redis.new(host: '127.0.0.1', port: 6379, db: REDIS_DB_NAME)
end

def uncompress_gz_file(file_name)
  infile = open(file_name)
  gz = Zlib::GzipReader.new(infile)
  res = gz.read
  gz.close
  res
end

def process_gz_file_content(input)
  res = {}
  input.each_line do |x|
    lng = x[0 .. 1].downcase
    ind1 = x.index(' ', 3)
    title = x[3 ... ind1]
    key = "#{lng}_#{title}"
    ind2 = x.index(' ', ind1 + 1)
    val = x[ind1 + 1 ... ind2]
    res[key] = val
  end

  res
end

def uncompress_and_process_gz_file(file_name)
  res = {}
  infile = open(file_name)
  gz = Zlib::GzipReader.new(infile)
  gz.each_line do |x|
    lng = x[0 .. 1].downcase
    ind1 = x.index(' ', 3)
    title = x[3 ... ind1]
    key = "#{lng}_#{title}"
    ind2 = x.index(' ', ind1 + 1)
    val = x[ind1 + 1 ... ind2]
    res[key] = val
  end

  res
end


# # todo - filter for only the records we need - wikipedia records
def process_pagecounts_dump_file(input, page_counts_hash, date_key)
  result = {}
  input.each do |lng_title, intern_id|

    # binding.pry

    if page_counts_hash[lng_title]
      count = page_counts_hash[lng_title].to_i
      lng = lng_title[0..1].to_sym


      #todo - refactor
      if result.key?(intern_id)
        
        if result[intern_id].key?(date_key)
          
          if result[intern_id][date_key].key?(lng)

            # binding.pry

            result[intern_id][date_key][lng] += count
          else
            result[intern_id][date_key][lng] = count
          end


        else
          result[intern_id][date_key] = {}
          result[intern_id][date_key][lng] = 0
        end
        

      else
        result[intern_id] = {}
        result[intern_id][date_key] = {}
        result[intern_id][date_key][lng] = 0
      end
    end
  end

  save_to_redis(result)
end

def save_to_redis(input)
  redis = connect_to_redis()
  i = 0
  input.each do |k, date_lng_val|

    #todo
    # if exist then merge
    # otherwise insert
    maybe_json = redis.get(k)
    date_key = date_lng_val.keys[0]
    if maybe_json

      binding.pry

      old_data = JSON.parse(maybe_json, { symbolize_names: true })
      new_data = old_data.merge(date_lng_val) do |key, v1, v2| 
        v1.merge(v2) { |key, v1, v2| v1 + v2 } 
      end

      redis.set(k, new_data.to_json) 
    else
      hash_formatted = {}
      hash_formatted[k] = date_lng_val
      redis.set(k, hash_formatted.to_json) #todo
    end
    
    puts("--The key #{k} has been saved to redis")
    i += 1
  end

  puts("-Total of #{i} keys has been saved to redis")
end

main()
# puts('Done')

# v.each do |date, lng_count_val|

    # end