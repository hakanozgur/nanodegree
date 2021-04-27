# Develop

- copy requirements into one
- .\venv\Scripts\activate
- insatll vs build tools
    - https://visualstudio.microsoft.com/thank-you-downloading-visual-studio/?sku=BuildTools&rel=16
- pip install -r requirements.txt

conda create -n ds python=3.7 anaconda

# Setup

conda activate ds
docker-compose up -d 
cd producers
python .\simulation.py 
cd consumers 
python .\connector.py 
faust -A faust_stream worker -l info 
python ksql.py 
python .\server.py

# Helpful

CURL -X DELETE localhost:8083/connectors/stations