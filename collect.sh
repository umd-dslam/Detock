DIR=4

mkdir -p ~/data/debug/$DIR/00
mkdir -p ~/data/debug/$DIR/01
mkdir -p ~/data/debug/$DIR/10
mkdir -p ~/data/debug/$DIR/11

build/release-counterless-ddr/client stats scheduler 4 --host 192.168.2.11 --truncate 100000 > ~/data/debug/$DIR/00/log.txt 
build/release-counterless-ddr/client stats scheduler 4 --host 192.168.2.12 --truncate 100000 > ~/data/debug/$DIR/01/log.txt 
build/release-counterless-ddr/client stats scheduler 4 --host 192.168.2.13 --truncate 100000 > ~/data/debug/$DIR/10/log.txt 
build/release-counterless-ddr/client stats scheduler 4 --host 192.168.2.14 --truncate 100000 > ~/data/debug/$DIR/11/log.txt 

build/release-counterless-ddr/client metrics /var/tmp --host 192.168.2.11
build/release-counterless-ddr/client metrics /var/tmp --host 192.168.2.12 
build/release-counterless-ddr/client metrics /var/tmp --host 192.168.2.13 
build/release-counterless-ddr/client metrics /var/tmp --host 192.168.2.14

scp 'vm-1:/var/tmp/*.csv' ~/data/debug/$DIR/00  
scp 'vm-2:/var/tmp/*.csv' ~/data/debug/$DIR/01  
scp 'vm-3:/var/tmp/*.csv' ~/data/debug/$DIR/10  
scp 'vm-4:/var/tmp/*.csv' ~/data/debug/$DIR/11 