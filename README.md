## Requirements

In order to run this batch it's mandatory to have installed JDK 8 and Apache Spark

### Installation guide for Debian distributions

#### JDK 8
From a terminal 

`sudo apt install openjdk-8-jdk`

if more than one jdk is installed, there could be problems. So please check.

`sudo update-alternatives --config java`

#### Apache Spark

Download Apache Spark

`wget https://apache.brunneis.com/spark/spark-2.4.5/spark-2.4.5-bin-hadoop2.7.tgz`

Extract downloaded file and move the content

`sudo tar xvf spark-2.3.1-bin-hadoop2.7.tgz -C /usr/local/spark`

`sudo mv /usr/local/spark/spark-2.3.1-bin-hadoop2.7/* /usr/local/spark/`

`sudo rm -rf /usr/local/spark/spark-2.3.1-bin-hadoop2.7`

Add Spark path to bash file

`nano ~/.bashrc`

Add below code snippet to the bash file

`SPARK_HOME=/usr/local/spark/`

`export PATH=$SPARK_HOME/bin:$PATH`

Execute below command after editing the bashsrc

`source ~/.bashrc`

## Launching app

My recommendation is to copy codeChallengeClarity-1.0-SNAPSHOT.jar allocated in target 
and code_challenge.conf allocated in src/main/resources for a more comfortable path
 
### 1. Parse the data with a time_init, time_end

`spark-submit --properties-file /path/to/code_challenge.conf 
--class ai.clarity.edt.launchers.ParseDataWithTimeLauncher$ 
/path/to/codeChallengeClarity-1.0-SNAPSHOT.jar /path/to/logs 
YYYY-MM-DDTHH:mm:SS YYYY-MM-DDTHH:mm:SS Hostname`

### 2. Unlimited Input Parser

For this option I've decided to take advantage of crontab.

`crontab -e`

Add this line and save

`0 * * * * /usr/local/spark/bin/spark-submit --master local[*] --properties-file code_challenge.conf 
--class ai.clarity.edt.launchers.UnlimitedInputParserLauncher /path/to/codeChallengeClarity-1.0-SNAPSHOT.jar
 /path/to/logs $(date +%s%3N) $(date --date="1 hour ago" +%s%3N)`
 
This command launches the App every hour on the hour.
 
#### App configuration
 
The application uses a configuration file to provide some configurable properties, these are:

<ul>
<li>spark.custom.log.level (custom log level) </li>
<li>spark.custom.result.path (path for writing results) </li>
<li>spark.custom.saveMode (writing mode) </li>
<li>spark.hostname.To (a list of hostnames connected to a given (configurable) host during the last hour) </li>
<li>spark.hostname.From (a list of hostnames received connections from a given (configurable) host during the last hour) </li>
</ul>

#### App results

The results will be written at the path configured in the following files:

<ul>
<li>firstGoal.txt: Results of (Parse the data with a time_init, time_end)</li>
<li>hostsConnectedTo.txt: Result of (a list of hostnames connected to a given (configurable) host during the last hour)</li>
<li>hostsConnectedFrom.txt: Results of (a list of hostnames received connections from a given (configurable) host during the last hour)</li>
<li>hostMostConnections.txt: Results of (the hostname that generated most connections in the last hour) </li>
</ul>
