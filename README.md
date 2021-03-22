# CS 6381 Distributed Systems Programming Assignment #2

Application and middleware for ZooKeeper and 0MQ

```
git clone https://github.com/thesammiller/cs6381_pa2.git  
```    

*Note: This assumes that Zookeeper is installed at /opt/zookeeper*

Required packages:    
```
sudo apt-get install mininet python3-zmq python3-kazoo python3-pip openvswitch-testcontroller
pip3 install mininet
sudo ln /usr/bin/ovs-testcontroller /usr/bin/controller 
```

To run:    
- Find a commands txt in `AutoTests-working`, `broker_setups` or `flood_setups` folder, e.g.`10p_10s.txt`
- Launch a mininet with the appropriate topology (i.e. `xp_yp.txt` needs x+y+1 hosts; AutoTests need 7 hosts)

```
sudo mn --topo single,21
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




