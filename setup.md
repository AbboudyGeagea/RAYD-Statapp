# 1. Create directory for Oracle
sudo mkdir -p /opt/oracle
cd /opt/oracle

# 2. Download and Unzip the Instant Client (Basic Light is usually enough)
sudo wget https://download.oracle.com/otn_software/linux/instantclient/1919000/instantclient-basiclite-linux.x64-19.19.0.0.0dbru.zip
sudo unzip instantclient-basiclite-linux.x64-19.19.0.0.0dbru.zip

# 3. Install libaio (required by Oracle)
sudo apt-get update && sudo apt-get install -y libaio1

# 4. Add to library path
echo "/opt/oracle/instantclient_19_19" | sudo tee /etc/ld.so.conf.d/oracle-instantclient.conf
sudo ldconfig
