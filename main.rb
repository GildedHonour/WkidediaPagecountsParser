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
    #   hour_formatted = '%02d' % hour
    #   file_url = PAGE_COUNTS_FILE_URL_TEMPLATE % { year: date_iter.year, month: date_iter.month, day: date_iter.day, hour: hour_formatted }
    #   local_file_name = 'pagecounts-%{year}%{month}%{day}-%{hour}0000.gz' % { year: date_iter.year, month: date_iter.month, day: date_iter.day, hour: hour } #todo - take the part after /
    #   save_file(file_url, local_file_name)
    #   local_file_name
    # end

    local_file_names = ['pagecounts-20141101-000000.gz'] #todo
    local_file_names.each do |file_name|
      puts("Processing #{file_name}...")
      page_counts_str = uncompress_gz_file("/Users/alex/Downloads/#{file_name}") #todo - path
      page_counts_hash = process_gz_file_content(page_counts_str)

      source_links = read_and_parse_csv(CSV_FILE_NAME)
      process_links_and_counts(source_links, page_counts_hash, date_iter)
      puts("Ok! Done processing #{file_name}")
    end
  end
end


def save_file(url, local_file_name)
  puts("Downloading #{url} ...")
  File.write(local_file_name, Net::HTTP.get(URI.parse(url)))
  puts("Ok!")
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
def process_links_and_counts(input, page_counts_hash, date_key)
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
  input.each do |k, v|
    date_key = v.keys[0]
    date_key_formatted = v.keys[0].strftime("%Y%m%d")

    hash_formatted = {}
    hash_formatted[date_key_formatted] = v[date_key]

    redis.set(k, hash_formatted.to_json) #todo
    puts("The key #{k} has been saved to redis")
    i += 1
  end

  puts("Total of #{i} keys has been saved to redis")
end

main()
# puts('Done')
