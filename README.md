# CS 6381 Distributed Systems Programming Assignment #2

Application and middleware for ZooKeeper and 0MQ

```
git clone https://github.com/thesammiller/cs6381_pa3.git 
```    

*Note: This assumes that Zookeeper is installed at /opt/zookeeper*

Required packages:    
```
sudo apt-get install mininet python3-zmq python3-kazoo python3-pip openvswitch-testcontroller
pip3 install mininet
sudo ln /usr/bin/ovs-testcontroller /usr/bin/controller 
```

To run:    
- Find a commands txt in `AutoTests-working`. If you want to run something from `broker_setups` or `flood_setups` folder then make sure to change line 26 in loadbalance.py to [8, 16] to increase the rebalancing threshold.
- Launch a mininet with the appropriate topology (all will work with 24 hosts - although this will leave some unused hosts for some of the tests);

```
sudo mn --topo single,24
mininet> source broker_setups/10p_10s.txt      
``` 
check `ps` on any host to see terminal output.     
check `logs/*.log` for time differential data.         

Run the following to generate graphs:    
```
sudo apt-get install -y python3-matplotlib python3-pandas
python3 graph_average.py
python3 graph_quantile.py
```




