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
- [ ] Roadmap Item - Mark index allocation to move data from hot to warm
- [ ] Roadmap Item - Identify indices not attached to a rollover
- [ ] Roadmap Item - Support auto migration of non-rollover attached indices to rollovers
- [ ] Roadmap Item - Support auto reindex of prior non-rollover data into rollover indices

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
```

Next, edit the **settings.toml** and **client.json** files. The **settings.toml** controls which ILM features are enabled as well as allows for fine tuning feature settings. The **client.json** includes Elasticsearch cluster information as well as policy information. The H & A Security Solutions Elastic ILM supports multiple elasticsearch clusters. To apply ILM to multiple clusters simply create more json files such as **client2.json**. Any \*.json file found in this project folder, by default, will be treated as a client file.

Finally, install the service.

```bash
sudo cp elastic-ilm.service /etc/systemd/system
sudo systemctl daemon-reload
sudo systemctl enable elastic-ilm
sudo service elastic-ilm start
```
