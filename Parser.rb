require 'open-uri'
require 'json/streamer'
require 'date'

=begin
Running this is pretty simple, you will first have to install the json streamer gem by running 
'$ gem install json-streamer'
Then you can just open an irb session loading this file by running 
'$ irb -r ./Parser.rb'
Then all you need to do is create a new parser object 
'irb(main):001> p = Parser.new()'

1. Downloads this file: https://summitroute.com/downloads/flaws_cloudtrail_logs.tar
2. Untars the file somewhere on your local drive.
3. Unzips each of the .json.gz files.
4. Iterates over the .json files and collect the unique AWS events per region.
5. And lastly prints out a simple report in this format:
=end

class Parser
  attr_accessor :region_event_hash, :tar_path, :logs_path, :tar_file

  #Saves instance variables and acts as cheap control flow for this class
  def initialize(url: 'https://summitroute.com/downloads/flaws_cloudtrail_logs.tar', logs_path: './log_files/', tar_path: './tar_files/', tar_file: 'logs.tar')
    @region_event_hash = Hash.new()
    @tar_path = tar_path
    @tar_file = tar_file
    @logs_path = logs_path

    start_process_time = Time.new()
    download_file(url)
    untar_unzip
    parse
    output_stats
    end_process_time = Time.new()
    puts "Entire process took #{end_process_time - start_process_time} seconds"
  end

  #creates dir if it does not exist yet and downloads .tar file
  def download_file(url)
    FileUtils.mkdir_p(self.tar_path) unless Dir.exist?(self.tar_path)
    puts "Downloading #{self.tar_file} to #{self.tar_path}"
    begin 
      download = URI.open(url)
      IO.copy_stream(download, "#{self.tar_path}#{self.tar_file}")
    rescue OpenURI => e
      raise "Error downloading file #{e.message}"
    end
  end

  #This system command untaring/unzipping process is dependent on having tar/gunzip installed
  #Also --strip-components=1 assumes that we know the consistent strucutre of the tar file we are getting
  #One could install a untaring gem but that felt pretty heavy handed for this process
  def untar_unzip
    FileUtils.mkdir_p(self.logs_path) unless Dir.exist?(self.logs_path)

    puts "Untaring #{self.tar_path}#{self.tar_file} to #{self.logs_path}"
    success_on_untar = system("tar -xf #{self.tar_path}#{self.tar_file} -C #{self.logs_path} --strip-components=1")
    raise "Failed to untar downloaded file, aborting" unless success_on_untar

    puts "Unzipping .gz files in #{self.logs_path}"
    Dir.glob("#{self.logs_path}*.gz") do |gz_file|
      success_on_unzip = system("gunzip #{gz_file}")
      raise "Failed to unzip file #{gz_file}, aborting" unless success_on_unzip
    end
  end

  #uses a stream to parse json files and stores resultes in a hash
  def parse
    puts "Starting to parse json files"
    Dir.glob("#{self.logs_path}*.json") do |json_filename|
      start_file_time = Time.new()

      File.open(json_filename, 'r') do |file_stream|
        streamer = Json::Streamer.parser(file_io: file_stream, chunk_size: 500)
  
        streamer.get(nesting_level:2) do |log_entry|
          region_key = log_entry["awsRegion"]
          event_key = log_entry["eventName"]
  
          if self.region_event_hash[region_key]
            self.region_event_hash[region_key][event_key] += 1
          else
            event_hash = Hash.new(0)
            event_hash[event_key] += 1
            self.region_event_hash[region_key] = event_hash
          end
        end
      end
      end_file_time = Time.new()
      puts "Processed #{json_filename} in #{end_file_time - start_file_time} seconds"
    end
  end

  #outpus contents of hash in sorted order
  def output_stats
    self.region_event_hash.keys.sort.each do |key|
      puts key
      self.region_event_hash[key].sort.each do |event_array|
        puts "#{event_array[0]}: #{event_array[1]}"
      end
    end
  end
end