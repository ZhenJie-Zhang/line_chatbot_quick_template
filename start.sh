#!bin/bash
sudo docker-compose up -d
sleep 5s
sudo curl $(docker port ngrok 4040)/api/tunnels > tunnels.json
sudo docker run -v $(pwd)/tunnels.json:/tmp/tunnels.json --rm  realguess/jq jq .tunnels[1].public_url /tmp/tunnels.json 
sudo docker exec -it jupyter bash
cd /root; jupyter notebook --allow-root &
