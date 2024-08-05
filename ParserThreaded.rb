require 'json/streamer'
require 'thread'

=begin
I originaly thought that using some threads would improve the perfomance of this program. 
However, I failed to realize that the program is in fact cpu bound when I orignally thought it was IO bound 
Since normal ruby (Matz's Ruby) is not truly multi-thread you get no perfromance improvment
I thought I would just leave this in here so we can talk about it if we want
You would have to of course write some aggreation function to print everything our coherently
=end

class ParserThreaded

  def parse()
    @region_hash = Queue.new()
    file_to_process = Queue.new 
    threads = []
    Dir.glob('./log_files/*.json').each do |json_filename|
      file_to_process << json_filename
    end

    8.times do 
      threads << Thread.new do

        until file_to_process.empty?
          current_file = file_to_process.pop(true) rescue nil

          if current_file

            region_hash = Hash.new()
            puts "Processing #{current_file}"
      
            file_stream = File.open(current_file, 'r')
            streamer = Json::Streamer.parser(file_io: file_stream, chunk_size: 500)
            
            streamer.get(nesting_level:2) do |log_entry|
              region_key = log_entry["awsRegion"]
              event_key = log_entry["eventName"]
            
              if region_hash[region_key]
                region_hash[region_key][event_key] += 1
              else
                event_hash = Hash.new(0)
                event_hash[event_key] += 1
                region_hash[region_key] = event_hash
              end
            end
            @region_hash << region_hash
          end
        end
      end
    end
    threads.each { |t| t.join }
  end


end






