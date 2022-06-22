# elastic-ilm
Alternative Index Lifecycle Management decoupled from Elastic for more granularity

## Why?
The Elastic Index Lifecycle Manager (ILM) is built-in to the Elastic basic license and above and easy to setup and use within Kibana. In our experience, however, the lifecycle policies sometimes break due to temporary performance issues as well as a few other causes. When such events happen, end users of ILM have to manually tell the index to retry its ILM policy. The retry process is burdensome and often requires going to Dev Tools or the command line to fix broken ILM stages.

In addition, decoupling from Elastic's ILM and using this projects custom ILM allows for more granularity and control. For example, rollover indices are only supported on Elastic Basic license or above. With this projects ILM, rollovers work even with the open source edition of Elastic. Here are other features of this project:

- [x] Rollover support
- [x] More granular rollover control (example - never rollover unless index is at least X GB in size)
- [x] Purge indices based on index creation date
- [x] Purge indices based on newest document within index
- [x] Generate accounting/billing information for index consumption with hot/warm tier pricing models
- [x] Mark index allocation to move data from hot to warm
- [ ] Considering - Identify indices not attached to a rollover
- [ ] Considering - Support auto migration of non-rollover attached indices to rollovers
- [ ] Considering - Support auto reindex of prior non-rollover data into rollover indices
- [ ] Considering - Support working in conjunction with Elastic's native ILM

:x: Do not use this ILM with Elastic's native ILM - They cannot co-exist

# Quickstart - Docker (Assumes Ubuntu OS as host)

:warning: If you wish to use a traditional install, skip the Docker section and go down to the **Quickstart Ubuntu 20.04 section.**

If you do not have docker installed, install it. The command to install Docker is usually one of the commands below. Find which one works on your system. Do not run them all unless you are doing trial and error.

```bash
# Debian-based OS
sudo apt install docker-ce
sudo apt install docker
sudo apt install docker.io
sudo apt install docker-compose

# RPM-based OS
sudo yum install docker-ce
sudo yum install docker
sudo yum install docker.io
sudo yum install docker-compose
```

Start by creating a **settings.toml** and **client.json** using this repositories **settings.toml.example** and **client.json.example** as starting configuration files. The example commands below pull down the examples and renames them. Do not forget to open and edit them.

```bash
wget https://raw.githubusercontent.com/HASecuritySolutions/elastic-ilm/main/client.json.example -O client.json
wget https://raw.githubusercontent.com/HASecuritySolutions/elastic-ilm/main/settings.toml.example -O settings.toml
```

Next, test deploy your container. This will launch it as an interactive terminal. Once you see it working you can press **CTRL+C** to kill the container. It will automatically be removed on stop due to **--rm**.

```bash
docker run -it --name elastic-ilm --rm -v ./settings.toml:/opt/elastic-ilm/settings.toml:ro -v ./client.json:/opt/elastic-ilm/client.json:ro hasecuritysolutions/elastic-ilm:latest
```

If everything ran correctly then feel free to deploy the container as an ongoing service with the command below.

```bash
docker run -d --name elastic-ilm -v ./settings.toml:/opt/elastic-ilm/settings.toml:ro -v ./client.json:/opt/elastic-ilm/client.json:ro hasecuritysolutions/elastic-ilm:latest
```

If you are planning on using the accounting feature, make sure to add a volume mount for the path you are saving accounting files. Example below. Replace **/opt/accounting:/opt/accounting** with the path to where you will be saving your accounting data.

```bash
docker run -d --name elastic-ilm -v ./settings.toml:/opt/elastic-ilm/settings.toml:ro -v ./client.json:/opt/elastic-ilm/client.json:ro -v /opt/accounting:/opt/accounting hasecuritysolutions/elastic-ilm:latest
```

# Quickstart - Assumes Ubuntu 20.04

First, clone and install the project's required python libraries:

```bash
sudo apt update
sudo apt install python3 python3-pip pipenv
cd /opt/
sudo git clone https://github.com/HASecuritySolutions/elastic-ilm.git
chown -R $USER:$USER elastic-ilm
cd /opt/elastic-ilm
pipenv install
cp settings.toml.example settings.toml
cp client.json.example client.json
mkdir /opt/elastic-ilm/accounting
```

Next, edit the **settings.toml** and **client.json** files. The **settings.toml** controls which ILM features are enabled as well as allows for fine tuning feature settings. The **client.json** includes Elasticsearch cluster information as well as policy information. The H & A Security Solutions Elastic ILM supports multiple elasticsearch clusters. To apply ILM to multiple clusters simply create more json files such as **client2.json**. Any \*.json file found in this project folder, by default, will be treated as a client file.

Finally, install the service.

```bash
sudo cp elastic-ilm.service /etc/systemd/system
sudo systemctl daemon-reload
sudo systemctl enable elastic-ilm
sudo service elastic-ilm start
```

This project is provided by H & A Security Solutions LLC. If you are interested in additional capabilities, professional engagements, or SIEM/NSM guidance, please reach out to info@hasecuritysolutions.com.

# Demo Workshop - Assumes you have docker, docker-compose, and pipenv installed (see guides above)

Note - The demo workshop assumes you are using /opt/elastic-ilm for your GitHub path. To do so you can do this:

```bash
sudo mkdir /opt/elastic-ilm -p
sudo chown -R $USER:$USER /opt/elastic-ilm # Gives the current user ownership of the folder
cd /opt/elastic-ilm
git clone https://github.com/HASecuritySolutions/elastic-ilm.git .
mkdir /opt/elastic-ilm/accounting
pipenv install
```

First, deploy an Elasticsearch and Kibana node to interact with. To do so, run the below commands from the demo folder of this GitHub repo.

```bash
cd /opt/elastic-ilm/demo
docker-compose up -d
```

Next, start Jupyter notebook using the command below.

```bash
cd /opt/elastic-ilm
pipenv run jupyter notebook --ip 0.0.0.0
```

You will see a string that looks similar to this:

```bash
To access the notebook, open this file in a browser:
        file:///home/jhenderson/.local/share/jupyter/runtime/nbserver-22963-open.html
    Or copy and paste one of these URLs:
        http://JHENDERSONDSK:8888/?token=c8de56fa4deed24899803e93c227592aef6538f93025fe01
     or http://127.0.0.1:8888/?token=c8de56fa4deed24899803e93c227592aef6538f93025fe01
```

You may get an error about "This command cannot be run due to an error. The system cannot find the file specified. Ignore the error and try connecting to http://localhost:8888. The page should load and ask for a password or token. Copy the token found after http://127.0.0.1:8888/?token= and paste it as the password. Then click **Log in**.

In the File Browser, click on and load **Demo.ipynb**.
