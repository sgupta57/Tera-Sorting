In this Project we have implemented Terasorting in 3 different ways as follows using Java, Hadoop and Spark. 
It is implemented such as to be able to perform external sort i.e sort files larger than disk size inplace. 
Then we have compared the performance with different types of implementations.


HADOOP:

ssh-keygen -f ~/.ssh/id_rsa -t rsa -P ""
cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys
sudo apt-get install mdadm
sudo umount -l /dev/xvdb
sudo mdadm --create --verbose /dev/md0 --level=0 --name=Saurabh --raid-devices=2 /dev/xvdb /dev/xvdc
sudo mkfs -t ext4  /dev/md0
sudo mkdir -p /Saurabh
sudo mount LABEL=Saurabh /Saurabh

sudo apt-get update
sudo apt-get install openjdk-7-jdk
wget http://apache.claz.org/hadoop/common/hadoop-2.7.4/hadoop-2.7.4.tar.gz
sudo tar -zxvf hadoop-2.7.4.tar.gz
sudo mv hadoop-2.7.4 hadoop


wget http://www.ordinal.com/try.cgi/gensort-linux-1.5.tar.gz
tar -zvxf gensort-linux-1.5.tar.gz

./gensort -a 1280000000 inputdata
hdfs dfs -put inputdata /input/

hadoop jar TeraSorting.jar TeraSorting /input/inputdata /Output/output



SPARK:

wget https://d3kbcqa49mib13.cloudfront.net/spark-2.2.0-bin-hadoop2.7.tgz
sudo tar zxvf spark-2.2.0-bin-hadoop2.7.tgz -C /opt

$SPARK_HOME/bin/spark-shell
scala> :load spark_sort.scala


SHARED MEMORY:
SharedMemory
To run the jC program 
gcc Shared_mem.c -o sharedmemory
./sharedmemory