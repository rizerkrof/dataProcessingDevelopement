ls
mls
ls
cat connect-file-sink.properties 
connect-standalone connect-standalone.properties connect-file-sink.properties
exit
cat test.sink.txt 
connect-standalone connect-standalone.properties connect-file-sink.properties
cat test.sink.txt 
exit
