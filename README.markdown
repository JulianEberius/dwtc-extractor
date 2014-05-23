## DWTC-Extractor: A Web Table Extractor for the Common Crawl

This is the complete extractor thas was used to create the [Dresden Web Table Corpus](http://wwwdb.inf.tu-dresden.de/misc/edyra/dwtc)1

*NOTE*: This is partially based on code that was originally released as part of the [Web Data Commons project](http://webdatacommons.org/). The original repository is found [here](https://www.assembla.com/code/commondata/subversion/nodes/3/Extractor/trunk/extractor).
<!-- It was modified for the extraction of the Dresden Web Table Corpus by Julian Eberius of the Database Technology Group at TU Dresden. -->

Modifications include:

- removing unnecessary pieces of code and updating dependencies
- integrating [JWAT](https://sbforge.org/display/JWAT/JWAT) into the original extraction code to be able to process newer versions of the Common Crawl that use the WARC file format. This also means that this extractor is a good basis for any extraction task on the new WARC-based versions of the Common Crawl.
- adding web table recognition and extraction code (in the package "webreduce")

Also note that the Web Data Commons project now publishes [its own web table corpus](http://webdatacommons.org/webtables/index.html), based on an older version of the Common Crawl.
This code was forked before the WDC corpus was published.

### Contents

- [Running the Extractor](#running)
- [Retrieving the data](#retrieving)
- [Implementing a new extractor](#newExtractor)

### <a name="running"></a> Running the Extractor (Original Documentation)

What follows are the original comments to this code by the original authors, edited where necessary. The documentation of the master script mostly still applies, or was edited to work with the new version of the CC.

This implementation extracts structured web data in various formats from the Common Crawl web corpus by using an AWS pipeline.
The data is given in an EC2 bucket and consists of a large number of web pages, which is split into a number of archive files.

The setup is to use a SQS queue for extraction tasks, where each queue entry contains a single data file.
A number of extraction EC2 instances monitors this queue, and performs the actual extraction. Results are again written into EC2 (data) and SDB (statistics).
Use as follows:

0.  Create a webreduce.properties file in /src/main/resources. A sample file is provided.

1.  Create a runnable JAR
This is done by running

        mvn install

    in this directory. This creates a JAR file webreduce-extractor-*.jar in the "target/" subdirectory.

2.  Use 'deploy' command to upload the JAR to S3

        ./bin/master deploy --jarfile target/webreduce-extractor-0.0.1-jar-with-dependencies.jar

3.  Use 'queue' command to fill the extraction queue with CC file names.

        ./bin/master queue --bucket-prefix 2010/08/09/0/

    For the new version of the CC, it is easiest to use the -f option to queue from a provided file, e.g.

        bin/master queue -f 2013segments


4.  Use 'start' command to launch EC2 extraction instances from the spot market. This request will keep starting instances until it is cancelled, so beware! Also, the price limit has to be given. The current spot prices can be found at http://aws.amazon.com/ec2/spot-instances/#6 . A general recommendation is to set this price at about the on-demand instance price. This way, we will benefit from the generally low spot prices without our extraction process being permanently killed. The price limit is given in US$.

        ./bin/master start --worker-amount 10 --pricelimit 0.6

    Note: it may take a while (observed: ca. 10-15 Minutes) for the instances to become available and start taking tasks from the queue.

5.  Wait until everything is finished using the 'monitor' command

        ./bin/master monitor

    The monitoring process will try to guess the extraction speed and give an estimate on the remaining time. Also, the number of currently running instances is displayed.

6.  Use 'shutdown' command to kill worker nodes and terminate the spot instance request

        ./bin/master shutdown


7.  Collect result data and statistics with the 'retrievedata' and 'retrievestats' command

        ./bin/master retrievedata --destination /some/directory
        ./bin/master retrievestats --destination /some/directory


    Both the data and the statistics will be stored in this directory

To reset the process, use the 'clearqueue' command to reset the data queue and 'cleardata' to remove intermediate extraction results and statistics

    ./bin/master clearqueue
    ./bin/master cleardata

Note that you have to wait 60 seconds before you can reissue the 'queue' command after 'clearqueue'


### <a name="retrieving"></a> Notes on retrieving the data (new)

The above will put a large number of small files into your bucket, one file for each file of the Common Crawl. Since it only extracts a small portion of each file, the files will be relatively small, too small to work with, for example, Hadoop. One good way to retrieve the many small files from S3 and store them as a smaller set of larger files is to use [S3DistCp](http://docs.aws.amazon.com/ElasticMapReduce/latest/DeveloperGuide/UsingEMR_s3distcp.html) with its groupBy option.

### <a name="newExtractor"></a> Creating a new extractor (new)

This extractor code can be used as a general way to easily iterate the Common Crawl.
It is also faster (and cheaper) than using Hadoop on EMR for example, if you don't need any reduce functionality.

Simply add your own extraction code (see the webreduce package for an example based on [JSoup](http://jsoup.org) as the HTML parser), then modify the class *Worker* to call it. The interesting methods there are run() for the extraction, upload(...) for the serialization of your extraction results, and makeOutputFileKey(...) for defining output file names.
