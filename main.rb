require 'net/http'
require 'csv'
require 'redis'
require 'date'
require 'pry'

PAGE_COUNTS_FILE_URL_TEMPLATE = 'https://dumps.wikimedia.org/other/pagecounts-raw/%{year}/%{year}-%{month}/pagecounts-%{year}%{month}%{day}-%{hour}0000.gz'
MIN_YEAR = 2007
HOURS_PER_DAY = 23
CSV_FILE_NAME = 'wiki_urls.csv'

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
    local_file_names = (0..HOURS_PER_DAY).map do |hour|
      hour_formatted = '%02d' % hour
      file_url = PAGE_COUNTS_FILE_URL_TEMPLATE % { year: date_iter.year, month: date_iter.month, day: date_iter.day, hour: hour_formatted }
      local_file_name = 'pagecounts-%{year}%{month}%{day}-%{hour}0000.gz' % { year: date_iter.year, month: date_iter.month, day: date_iter.day, hour: hour } #todo - take the part after /
      save_file(file_url, local_file_name)
      local_file_name
    end

    local_file_names.each do |file_name|
      page_counts_str = uncompress_gz_file("/Users/alex/Downloads/#{file_name}") #todo - path
      page_counts_hash = process_gz_file_content(page_counts_str)

      source_links = read_and_parse_csv(CSV_FILE_NAME)
      process_links_and_counts(date_iter, source_links)
    end
  end
end


def save_file(url, local_file_name)
  File.write(local_file_name, Net::HTTP.get(URI.parse(url)))
  puts("A file from #{url} downloaded and saved.")
end



#todo remove
def save_files_by_dates(s_date, e_date: nil)
  to_date = e_date ? e_date : s_date
  (0 .. (to_date - s_date).to_i).to_a.each do |x|
    d = e_date + x #todo - d - rename
    (0 .. HOURS_PER_DAY).to_a.each do |hour|
      file_url = PAGE_COUNTS_FILE_URL_TEMPLATE % { year: d.year, month: d.month, day: d.day, hour: hour } #todo - make x2 00..23
      local_file_name = 'pagecounts-%{year}%{month}%{day}-%{hour}0000.gz' % { year: d.year, month: d.month, day: d.day, hour: hour } #todo - take the part after /
      save_file(file_url, file_name)
    end
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
        title = maybe_link[maybe_link.rindex('/') + 1 .. -1]
        next if title.include?('?curid=')
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
  redis = Redis.new(host: '127.0.0.1', port: 6380, db: 'wikipedia_pagecounts')
end

def uncompress_gz_file(file_name)
  infile = open(file_name)
  gz = Zlib::GzipReader.new(infile)
  res = gz.read
  gz.close
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




# # todo - filter for only the records we need - wikipedia records
def process_links_and_counts(date_key, input)
  result = {}
  input.each do |lng_title, intern_id|
    if page_counts_hash[lng_title]
      val = page_counts_hash[lng_title].to_i
      lng = lng_title[0..1].to_sym


      #todo - refactor
      if result.key?(intern_id)
        
        if result[intern_id].key?(date_key)
          
          if result[intern_id][date_key].key?(lng)

            # binding.pry

            result[intern_id][date_key][lng] += val
          else
            result[intern_id][date_key][lng] = val
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
end

main()
# puts('Done')
