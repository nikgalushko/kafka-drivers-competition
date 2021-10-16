go build
records=( 10000 50000 100000 200000 500000 )
for i in "${records[@]}"
do
  ./kafka-drivers-competition -driver=segmentio -strategy=produce -records=$i
  ./kafka-drivers-competition -driver=sarama-cluster -strategy=produce -records=$i
  echo "--------------------------------------------------------------------------------------------"
  ./kafka-drivers-competition -driver=segmentio -strategy=consume -records=$i
  ./kafka-drivers-competition -driver=sarama-cluster -strategy=consume -records=$i
done