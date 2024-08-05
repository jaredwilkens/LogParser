Running this is pretty simple, you will first have to install the json streamer gem by running 
'$ gem install json-streamer'
Then you can just open an irb session loading this file by running 
'$ irb -r ./Parser.rb'
Then all you need to do is create a new parser object 
'irb(main):001> p = Parser.new()'

Instructions
1. Downloads this file: https://summitroute.com/downloads/flaws_cloudtrail_logs.tar
2. Untars the file somewhere on your local drive.
3. Unzips each of the .json.gz files.
4. Iterates over the .json files and collect the unique AWS events per region.
5. And lastly prints out a simple report in this format:
