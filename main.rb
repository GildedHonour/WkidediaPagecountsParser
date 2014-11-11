require 'net/http'
require 'csv'
require 'redis'
require 'date'
require 'pry'

PAGE_COUNTS_FILE_URL_TEMPLATE = 'https://dumps.wikimedia.org/other/pagecounts-raw/%{year}/%{year}-%{month}/pagecounts-%{year}%{month}%{day}-%{hour}0000.gz'
MIN_YEAR = 2007
HOURS_PER_DAY = 23

def main
  date_start = Date.parse(ARGV[0])
  abort("[ERROR] Start date cannot be let than #{MIN_YEAR} year.") if date_start.year < MIN_YEAR
  if ARGV[1]
    date_end = Date.parse(ARGV[1]) 
    abort('[ERROR] Start date cannot be more than End date.') if date_start > date_end
  end 
  
  abort('[ERROR] Start date has to be less or equal to the current date.') if date_start >= DateTime.now.to_date
rescue
  puts('[ERROR] Invalid format for the date(s). Specify the start and end dates in the format YYYYmmdd.')
else
  puts("Ok")
  puts(date_start)
  puts(date_end) if date_end
end

def save_file(url, local_file_name)
  File.write(local_file_name, Net::HTTP.get(URI.parse(url)))
  puts("A file from #{url} downloaded and saved.")
end

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
    row_result = {}
    first_url_index = row.index { |x| x.start_with?('http://') }
    row.drop(first_url_index).each do |maybe_link|
      link_result = {}
      if maybe_link.start_with?('http://')
        lng = maybe_link[7..8]
        title = maybe_link[maybe_link.rindex('/') + 1 .. -1]
        next if title.include?('?curid=')

        lng_key = lng.to_sym
        if row_result.has_key?(lng_key)
          row_result[lng_key][:titles] << title
        else
          row_result[lng_key] = { count: 0 }
          row_result[lng_key][:titles] = [title]
        end
      else
        row_result[row_result.keys[-1]][:titles][-1] = ',' + maybe_link
      end
    end

    result[row[0].to_sym] = row_result
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


# save_file('https://avatars0.githubusercontent.com/u/2032888?v=3&s=460', 'test2_img.jpg')
# main()
links = read_and_parse_csv('wiki_urls.csv')
page_counts_str = uncompress_gz_file('/Users/alex/Downloads/pagecounts-20141101-000000.gz')


links.each do |k, v|

  v.each do |lng, titles_count|
    titles_count[:titles].each do |title|
      template = "#{lng} #{title}"
      first_ind = page_counts_str.index(template)
      if first_ind
                  # binding.pry
        last_index = page_counts_str.index("\n", first_ind) 
        value = page_counts_str[first_ind ... last_index].split[2].to_i
        puts('Value is ' + value.to_s) #todo remove
      end

    end 
  end

  # insert into Redis
end